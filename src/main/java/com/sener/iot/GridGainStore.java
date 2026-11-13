package com.sener.iot;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * All GridGain 9.2 access lives here:
 * <ul>
 *   <li>schema creation with SQL DDL (distribution zone + colocated time-series tables),</li>
 *   <li>high-rate writes through the Data Streamer API (one long-lived streamer per table),</li>
 *   <li>SQL queries for history, row counts and retention purge.</li>
 * </ul>
 */
public final class GridGainStore implements AutoCloseable {
    private final Config cfg;
    private final IgniteClient client;

    private RecordView<Tuple> readingView;
    private RecordView<Tuple> latestView;
    private RecordView<Tuple> statsView;

    private volatile SubmissionPublisher<DataStreamerItem<Tuple>> readingPub;
    private volatile SubmissionPublisher<DataStreamerItem<Tuple>> latestPub;
    private volatile CompletableFuture<Void> readingFuture;
    private volatile CompletableFuture<Void> latestFuture;
    private volatile boolean closing;

    public final AtomicLong submitted = new AtomicLong();
    public final AtomicLong dropped = new AtomicLong();
    public final AtomicLong streamerRestarts = new AtomicLong();
    public volatile String lastError = "";

    public GridGainStore(Config cfg) {
        this.cfg = cfg;
        this.client = IgniteClient.builder()
                .addresses(cfg.addresses().toArray(new String[0]))
                .connectTimeout(5_000)
                .build();
    }

    // ------------------------------------------------------------------ schema

    public void createSchema() {
        var sql = client.sql();
        try {
            sql.execute("CREATE ZONE IF NOT EXISTS IOT_ZONE (PARTITIONS " + cfg.partitions() + ", REPLICAS "
                    + cfg.replicas() + ") STORAGE PROFILES ['default']").close();
        } catch (Exception e) {
            // older syntax fallback
            sql.execute("CREATE ZONE IF NOT EXISTS IOT_ZONE WITH STORAGE_PROFILES='default', PARTITIONS="
                    + cfg.partitions() + ", REPLICAS=" + cfg.replicas()).close();
        }
        // keep an existing zone in line with the configured replica count (idempotent; triggers rebalancing)
        try {
            sql.execute("ALTER ZONE IOT_ZONE SET (REPLICAS " + cfg.replicas() + ")").close();
        } catch (Exception e) {
            try {
                sql.execute("ALTER ZONE IOT_ZONE SET REPLICAS=" + cfg.replicas()).close();
            } catch (Exception e2) {
                System.err.println("[gridgain] could not set zone replicas: " + e2.getMessage());
            }
        }
        String[] ddl = {
            // sensor registry (reference data)
            "CREATE TABLE IF NOT EXISTS SENSOR (SENSOR_ID VARCHAR PRIMARY KEY, SENSOR_NAME VARCHAR, "
                + "STREAM_TYPE VARCHAR, FLOOR_NO INT, ZONE_NAME VARCHAR, UNIT VARCHAR) ZONE IOT_ZONE",
            // most recent value per sensor (upserted by the streamer)
            "CREATE TABLE IF NOT EXISTS SENSOR_LATEST (SENSOR_ID VARCHAR PRIMARY KEY, "
                + "TS TIMESTAMP WITH LOCAL TIME ZONE, VAL DOUBLE, ATTR VARCHAR) ZONE IOT_ZONE",
            // full time series, colocated by sensor so one sensor's history lives in one partition
            "CREATE TABLE IF NOT EXISTS SENSOR_READING (SENSOR_ID VARCHAR, TS TIMESTAMP WITH LOCAL TIME ZONE, "
                + "VAL DOUBLE, ATTR VARCHAR, PRIMARY KEY (SENSOR_ID, TS)) COLOCATE BY (SENSOR_ID) ZONE IOT_ZONE",
            "CREATE INDEX IF NOT EXISTS IDX_READING_SENSOR_TS ON SENSOR_READING USING SORTED (SENSOR_ID, TS)",
            "CREATE INDEX IF NOT EXISTS IDX_READING_TS ON SENSOR_READING USING SORTED (TS)",
            // per-stream ingest statistics, persisted every few seconds
            "CREATE TABLE IF NOT EXISTS INGEST_STATS (STREAM_TYPE VARCHAR, TS TIMESTAMP WITH LOCAL TIME ZONE, "
                + "RECEIVED BIGINT, RATE_PER_SEC DOUBLE, REJECTED BIGINT, PRIMARY KEY (STREAM_TYPE, TS)) "
                + "COLOCATE BY (STREAM_TYPE) ZONE IOT_ZONE"
        };
        for (String s : ddl) {
            sql.execute(s).close();
        }
        readingView = client.tables().table("SENSOR_READING").recordView();
        latestView = client.tables().table("SENSOR_LATEST").recordView();
        statsView = client.tables().table("INGEST_STATS").recordView();
    }

    public void registerSensors(List<Sensor> sensors) {
        RecordView<Tuple> v = client.tables().table("SENSOR").recordView();
        List<Tuple> rows = new ArrayList<>();
        for (Sensor s : sensors) {
            rows.add(Tuple.create().set("SENSOR_ID", s.id()).set("SENSOR_NAME", s.name())
                    .set("STREAM_TYPE", s.type().name()).set("FLOOR_NO", s.floor())
                    .set("ZONE_NAME", s.zone()).set("UNIT", s.unit()));
        }
        v.upsertAll(null, rows);
    }

    // --------------------------------------------------------------- streaming

    /** Starts the two long-lived Data Streamer pipelines (readings + latest values). */
    public synchronized void startStreamers() {
        DataStreamerOptions opts = DataStreamerOptions.builder()
                .pageSize(500)
                .perPartitionParallelOperations(2)
                .autoFlushInterval(250)
                .retryLimit(8)
                .build();

        SubmissionPublisher<DataStreamerItem<Tuple>> rp = new SubmissionPublisher<>(ForkJoinPool.commonPool(), 100_000);
        SubmissionPublisher<DataStreamerItem<Tuple>> lp = new SubmissionPublisher<>(ForkJoinPool.commonPool(), 100_000);
        readingFuture = readingView.streamData(rp, opts);
        latestFuture = latestView.streamData(lp, opts);
        readingPub = rp;
        latestPub = lp;
        readingFuture.whenComplete(onEnd("readings"));
        latestFuture.whenComplete(onEnd("latest"));
    }

    private BiConsumer<Void, Throwable> onEnd(String name) {
        return (v, err) -> {
            if (closing) return;
            lastError = name + " streamer ended: " + (err == null ? "completed" : String.valueOf(err.getCause() != null ? err.getCause() : err));
            System.err.println("[gridgain] " + lastError + " - restarting in 3s");
            CompletableFuture.delayedExecutor(3, java.util.concurrent.TimeUnit.SECONDS).execute(() -> {
                if (!closing) {
                    streamerRestarts.incrementAndGet();
                    try {
                        startStreamers();
                    } catch (Exception e) {
                        lastError = "restart failed: " + e;
                        CompletableFuture.delayedExecutor(3, java.util.concurrent.TimeUnit.SECONDS)
                                .execute(() -> onEnd(name).accept(null, e));
                    }
                }
            });
        };
    }

    /** Queues one reading for both tables. Blocks (back-pressure) only if the buffers are completely full. */
    public void write(Sensor s, Reading r) {
        SubmissionPublisher<DataStreamerItem<Tuple>> rp = readingPub;
        SubmissionPublisher<DataStreamerItem<Tuple>> lp = latestPub;
        if (rp == null || lp == null || rp.isClosed()) {
            dropped.incrementAndGet();
            return;
        }
        Instant ts = Instant.ofEpochMilli(r.tsMs());
        rp.submit(DataStreamerItem.of(Tuple.create().set("SENSOR_ID", s.id()).set("TS", ts)
                .set("VAL", r.value()).set("ATTR", r.attr())));
        lp.submit(DataStreamerItem.of(Tuple.create().set("SENSOR_ID", s.id()).set("TS", ts)
                .set("VAL", r.value()).set("ATTR", r.attr())));
        submitted.incrementAndGet();
    }

    /** Items accepted by the publishers but not yet consumed by the streamer (rough queue depth). */
    public int queueLag() {
        SubmissionPublisher<DataStreamerItem<Tuple>> rp = readingPub;
        return rp == null ? 0 : Math.max(0, rp.estimateMaximumLag());
    }

    public boolean streamersRunning() {
        CompletableFuture<Void> a = readingFuture, b = latestFuture;
        return a != null && b != null && !a.isDone() && !b.isDone();
    }

    // ----------------------------------------------------------------- queries

    public record Latest(String id, long tsMs, double value, String attr) {
    }

    public List<Latest> loadLatest() {
        List<Latest> out = new ArrayList<>();
        try (ResultSet<SqlRow> rs = client.sql().execute("SELECT SENSOR_ID, TS, VAL, ATTR FROM SENSOR_LATEST")) {
            while (rs.hasNext()) {
                SqlRow r = rs.next();
                out.add(new Latest(r.stringValue(0), r.timestampValue(1).toEpochMilli(), r.doubleValue(2), r.stringValue(3)));
            }
        }
        return out;
    }

    /** Streams recent readings from GridGain (used to seed the in-memory trend cache after a restart). */
    public void scanSince(Instant since, java.util.function.Consumer<Reading> sink) {
        try (ResultSet<SqlRow> rs = client.sql().execute(
                "SELECT SENSOR_ID, TS, VAL FROM SENSOR_READING WHERE TS >= ?", since)) {
            while (rs.hasNext()) {
                SqlRow r = rs.next();
                sink.accept(new Reading(r.stringValue(0), r.timestampValue(1).toEpochMilli(), r.doubleValue(2), null));
            }
        }
    }

    /** Raw history for one sensor, downsampled to at most maxPoints (min/avg/max per bucket). */
    public List<double[]> history(String sensorId, Instant since, int maxPoints) {
        List<long[]> ts = new ArrayList<>();
        List<Double> vals = new ArrayList<>();
        try (ResultSet<SqlRow> rs = client.sql().execute(
                "SELECT TS, VAL FROM SENSOR_READING WHERE SENSOR_ID = ? AND TS >= ? ORDER BY TS", sensorId, since)) {
            while (rs.hasNext()) {
                SqlRow r = rs.next();
                ts.add(new long[]{r.timestampValue(0).toEpochMilli()});
                vals.add(r.doubleValue(1));
            }
        }
        List<double[]> out = new ArrayList<>();
        int n = ts.size();
        if (n == 0) return out;
        int per = Math.max(1, (int) Math.ceil(n / (double) maxPoints));
        for (int i = 0; i < n; i += per) {
            int end = Math.min(n, i + per);
            double sum = 0, mn = Double.MAX_VALUE, mx = -Double.MAX_VALUE;
            for (int k = i; k < end; k++) {
                double v = vals.get(k);
                sum += v;
                mn = Math.min(mn, v);
                mx = Math.max(mx, v);
            }
            out.add(new double[]{ts.get(i)[0], sum / (end - i), mn, mx});
        }
        return out;
    }

    /** Per-sensor aggregates over a recent window, computed inside GridGain: {avg, min, max, count}. */
    public java.util.Map<String, double[]> aggregates(Instant since) {
        java.util.Map<String, double[]> out = new java.util.HashMap<>();
        try (ResultSet<SqlRow> rs = client.sql().execute(
                "SELECT SENSOR_ID, AVG(VAL), MIN(VAL), MAX(VAL), COUNT(*) FROM SENSOR_READING WHERE TS >= ? GROUP BY SENSOR_ID", since)) {
            while (rs.hasNext()) {
                SqlRow r = rs.next();
                out.put(r.stringValue(0), new double[]{r.doubleValue(1), r.doubleValue(2), r.doubleValue(3), r.longValue(4)});
            }
        }
        return out;
    }

    public long rowCount() {
        try (ResultSet<SqlRow> rs = client.sql().execute("SELECT COUNT(*) FROM SENSOR_READING")) {
            return rs.hasNext() ? rs.next().longValue(0) : 0;
        }
    }

    public void purgeOlderThan(Instant cutoff) {
        client.sql().execute("DELETE FROM SENSOR_READING WHERE TS < ?", cutoff).close();
        client.sql().execute("DELETE FROM INGEST_STATS WHERE TS < ?", cutoff).close();
    }

    public void saveStats(Collection<Tuple> rows) {
        statsView.upsertAll(null, rows);
    }

    public List<String> nodeNames() {
        List<String> n = new ArrayList<>();
        try {
            client.cluster().nodes().forEach(x -> n.add(x.name()));
        } catch (Exception e) {
            // not connected
        }
        return n;
    }

    public List<String> connectedNodes() {
        List<String> n = new ArrayList<>();
        client.connections().forEach(x -> n.add(x.name()));
        return n;
    }

    @Override
    public void close() {
        closing = true;
        SubmissionPublisher<DataStreamerItem<Tuple>> rp = readingPub, lp = latestPub;
        if (rp != null) rp.close();
        if (lp != null) lp.close();
        try {
            if (readingFuture != null) readingFuture.orTimeout(10, java.util.concurrent.TimeUnit.SECONDS).join();
            if (latestFuture != null) latestFuture.orTimeout(10, java.util.concurrent.TimeUnit.SECONDS).join();
        } catch (Exception ignored) {
        }
        client.close();
    }
}
