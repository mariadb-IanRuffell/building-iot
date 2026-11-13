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

    /** In-memory (aimem) table: the last RETENTION_HOURS of readings. Disk table SENSOR_READING keeps everything. */
    public static final String HOT = "SENSOR_READING_HOT";
    public static final String HISTORY = "SENSOR_READING";
    public static final String MEM_PROFILE = "aimem_hot";

    private RecordView<Tuple> statsView;
    private final long retentionMinutes;

    /** One long-lived Data Streamer pipeline into one table. */
    private static final class Pipe {
        final String name;
        RecordView<Tuple> view;
        volatile SubmissionPublisher<DataStreamerItem<Tuple>> pub;
        volatile CompletableFuture<Void> fut;

        Pipe(String name) {
            this.name = name;
        }
    }

    private final Pipe hotPipe = new Pipe(HOT);
    private final Pipe histPipe = new Pipe(HISTORY);
    private final Pipe latestPipe = new Pipe("SENSOR_LATEST");
    private final Pipe[] pipes = {hotPipe, histPipe, latestPipe};
    private volatile boolean closing;

    public final AtomicLong submitted = new AtomicLong();
    public final AtomicLong dropped = new AtomicLong();
    public final AtomicLong streamerRestarts = new AtomicLong();
    public volatile String lastError = "";

    public GridGainStore(Config cfg) {
        this.cfg = cfg;
        this.retentionMinutes = cfg.retentionHours() * 60L;
        this.client = IgniteClient.builder()
                .addresses(cfg.addresses().toArray(new String[0]))
                .connectTimeout(5_000)
                .build();
    }

    // ------------------------------------------------------------------ schema

    public long retentionMinutes() {
        return retentionMinutes;
    }

    private void createZone(String name, String profile) {
        var sql = client.sql();
        try {
            sql.execute("CREATE ZONE IF NOT EXISTS " + name + " (PARTITIONS " + cfg.partitions() + ", REPLICAS "
                    + cfg.replicas() + ") STORAGE PROFILES ['" + profile + "']").close();
        } catch (Exception e) {
            // older syntax fallback
            sql.execute("CREATE ZONE IF NOT EXISTS " + name + " WITH STORAGE_PROFILES='" + profile + "', PARTITIONS="
                    + cfg.partitions() + ", REPLICAS=" + cfg.replicas()).close();
        }
        // keep an existing zone in line with the configured replica count (idempotent; triggers rebalancing)
        try {
            sql.execute("ALTER ZONE " + name + " SET (REPLICAS " + cfg.replicas() + ")").close();
        } catch (Exception e) {
            try {
                sql.execute("ALTER ZONE " + name + " SET REPLICAS=" + cfg.replicas()).close();
            } catch (Exception e2) {
                System.err.println("[gridgain] could not set replicas of " + name + ": " + e2.getMessage());
            }
        }
    }

    public void createSchema() {
        var sql = client.sql();
        createZone("IOT_ZONE", "default");          // disk (aipersist): reference data, latest values, full history
        createZone("IOT_MEM_ZONE", MEM_PROFILE);    // memory (aimem): hot window
        String cols = "(SENSOR_ID VARCHAR, TS TIMESTAMP WITH LOCAL TIME ZONE, VAL DOUBLE, ATTR VARCHAR, PRIMARY KEY (SENSOR_ID, TS)) "
                + "COLOCATE BY (SENSOR_ID) ZONE ";
        String[] ddl = {
            "CREATE TABLE IF NOT EXISTS SENSOR (SENSOR_ID VARCHAR PRIMARY KEY, SENSOR_NAME VARCHAR, "
                + "STREAM_TYPE VARCHAR, FLOOR_NO INT, ZONE_NAME VARCHAR, UNIT VARCHAR) ZONE IOT_ZONE",
            "CREATE TABLE IF NOT EXISTS SENSOR_LATEST (SENSOR_ID VARCHAR PRIMARY KEY, "
                + "TS TIMESTAMP WITH LOCAL TIME ZONE, VAL DOUBLE, ATTR VARCHAR) ZONE IOT_ZONE",
            // ALL history, on disk, never purged
            "CREATE TABLE IF NOT EXISTS " + HISTORY + " " + cols + "IOT_ZONE",
            "CREATE INDEX IF NOT EXISTS IDX_READING_SENSOR_TS ON " + HISTORY + " USING SORTED (SENSOR_ID, TS)",
            "DROP INDEX IF EXISTS IDX_READING_TS",
            // last N hours, in memory, purged continuously
            "CREATE TABLE IF NOT EXISTS " + HOT + " " + cols + "IOT_MEM_ZONE",
            "CREATE INDEX IF NOT EXISTS IDX_HOT_SENSOR_TS ON " + HOT + " USING SORTED (SENSOR_ID, TS)",
            "CREATE INDEX IF NOT EXISTS IDX_HOT_TS ON " + HOT + " USING SORTED (TS)",
            "CREATE TABLE IF NOT EXISTS INGEST_STATS (STREAM_TYPE VARCHAR, TS TIMESTAMP WITH LOCAL TIME ZONE, "
                + "RECEIVED BIGINT, RATE_PER_SEC DOUBLE, REJECTED BIGINT, PRIMARY KEY (STREAM_TYPE, TS)) "
                + "COLOCATE BY (STREAM_TYPE) ZONE IOT_ZONE"
        };
        for (String s : ddl) {
            sql.execute(s).close();
        }
        hotPipe.view = client.tables().table(HOT).recordView();
        histPipe.view = client.tables().table(HISTORY).recordView();
        latestPipe.view = client.tables().table("SENSOR_LATEST").recordView();
        statsView = client.tables().table("INGEST_STATS").recordView();
    }

    /**
     * Memory is volatile: if the hot table is empty (first run after adding the tier, or after all nodes restarted)
     * refill it from the disk history so the last N hours are available in memory again.
     * @return rows copied
     */
    public long backfillHotFromDisk() {
        if (hotRowCount() > 0) return 0;
        Instant from = Instant.now().minusSeconds(retentionMinutes * 60);
        client.sql().execute("INSERT INTO " + HOT + " SELECT SENSOR_ID, TS, VAL, ATTR FROM " + HISTORY + " WHERE TS >= ?", from).close();
        return hotRowCount();
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

    /** Starts the long-lived Data Streamer pipelines (hot table, disk history, latest values). */
    public synchronized void startStreamers() {
        DataStreamerOptions opts = DataStreamerOptions.builder()
                .pageSize(500)
                .perPartitionParallelOperations(2)
                .autoFlushInterval(250)
                .retryLimit(8)
                .build();
        for (Pipe p : pipes) {
            SubmissionPublisher<DataStreamerItem<Tuple>> pub = new SubmissionPublisher<>(ForkJoinPool.commonPool(), 100_000);
            p.fut = p.view.streamData(pub, opts);
            p.pub = pub;
            p.fut.whenComplete(onEnd(p.name));
        }
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

    /** Queues one reading for all three tables. Blocks (back-pressure) only if the buffers are completely full. */
    public void write(Sensor s, Reading r) {
        for (Pipe p : pipes) {
            if (p.pub == null || p.pub.isClosed()) {
                dropped.incrementAndGet();
                return;
            }
        }
        Instant ts = Instant.ofEpochMilli(r.tsMs());
        for (Pipe p : pipes) {
            p.pub.submit(DataStreamerItem.of(Tuple.create().set("SENSOR_ID", s.id()).set("TS", ts)
                    .set("VAL", r.value()).set("ATTR", r.attr())));
        }
        submitted.incrementAndGet();
    }

    /** Items accepted by the publishers but not yet consumed by the streamer (rough queue depth). */
    public int queueLag() {
        int max = 0;
        for (Pipe p : pipes) {
            var pub = p.pub;
            if (pub != null) max = Math.max(max, pub.estimateMaximumLag());
        }
        return max;
    }

    public boolean streamersRunning() {
        for (Pipe p : pipes) if (p.fut == null || p.fut.isDone()) return false;
        return true;
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
                "SELECT SENSOR_ID, TS, VAL FROM " + HOT + " WHERE TS >= ?", since)) {
            while (rs.hasNext()) {
                SqlRow r = rs.next();
                sink.accept(new Reading(r.stringValue(0), r.timestampValue(1).toEpochMilli(), r.doubleValue(2), null));
            }
        }
    }

    /**
     * History for one sensor as [ts, avg, min, max] rows, at most maxPoints.
     * Windows inside the retention period are served from the in-memory table (raw rows); longer windows come
     * from the disk table, pre-aggregated per minute (or hour for > 3 days) inside GridGain.
     */
    public List<double[]> history(String sensorId, Instant since, int maxPoints, boolean fromMemory) {
        List<double[]> rows = new ArrayList<>();      // {ts, avg, min, max}
        if (fromMemory) {
            try (ResultSet<SqlRow> rs = client.sql().execute(
                    "SELECT TS, VAL FROM " + HOT + " WHERE SENSOR_ID = ? AND TS >= ? ORDER BY TS", sensorId, since)) {
                while (rs.hasNext()) {
                    SqlRow r = rs.next();
                    double v = r.doubleValue(1);
                    rows.add(new double[]{r.timestampValue(0).toEpochMilli(), v, v, v});
                }
            }
        } else {
            String unit = java.time.Duration.between(since, Instant.now()).toDays() > 3 ? "HOUR" : "MINUTE";
            try (ResultSet<SqlRow> rs = client.sql().execute(
                    "SELECT FLOOR(TS TO " + unit + ") AS B, AVG(VAL), MIN(VAL), MAX(VAL) FROM " + HISTORY
                            + " WHERE SENSOR_ID = ? AND TS >= ? GROUP BY FLOOR(TS TO " + unit + ") ORDER BY B", sensorId, since)) {
                while (rs.hasNext()) {
                    SqlRow r = rs.next();
                    rows.add(new double[]{r.timestampValue(0).toEpochMilli(), r.doubleValue(1), r.doubleValue(2), r.doubleValue(3)});
                }
            }
        }
        int n = rows.size();
        if (n <= maxPoints) return rows;
        int per = (int) Math.ceil(n / (double) maxPoints);
        List<double[]> out = new ArrayList<>();
        for (int i = 0; i < n; i += per) {
            int end = Math.min(n, i + per);
            double sum = 0, mn = Double.MAX_VALUE, mx = -Double.MAX_VALUE;
            for (int k = i; k < end; k++) {
                sum += rows.get(k)[1];
                mn = Math.min(mn, rows.get(k)[2]);
                mx = Math.max(mx, rows.get(k)[3]);
            }
            out.add(new double[]{rows.get(i)[0], sum / (end - i), mn, mx});
        }
        return out;
    }

    /** Per-sensor aggregates over a recent window, computed inside GridGain: {avg, min, max, count}. */
    public java.util.Map<String, double[]> aggregates(Instant since) {
        java.util.Map<String, double[]> out = new java.util.HashMap<>();
        try (ResultSet<SqlRow> rs = client.sql().execute(
                "SELECT SENSOR_ID, AVG(VAL), MIN(VAL), MAX(VAL), COUNT(*) FROM " + HOT + " WHERE TS >= ? GROUP BY SENSOR_ID", since)) {
            while (rs.hasNext()) {
                SqlRow r = rs.next();
                out.put(r.stringValue(0), new double[]{r.doubleValue(1), r.doubleValue(2), r.doubleValue(3), r.longValue(4)});
            }
        }
        return out;
    }

    private long count(String table) {
        try (ResultSet<SqlRow> rs = client.sql().execute("SELECT COUNT(*) FROM " + table)) {
            return rs.hasNext() ? rs.next().longValue(0) : 0;
        }
    }

    public long hotRowCount() {
        return count(HOT);
    }

    public long diskRowCount() {
        return count(HISTORY);
    }

    /** Retention applies to the in-memory tier only; the disk history is never purged. */
    public void purgeHotOlderThan(Instant cutoff) {
        client.sql().execute("DELETE FROM " + HOT + " WHERE TS < ?", cutoff).close();
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
        for (Pipe p : pipes) if (p.pub != null) p.pub.close();
        try {
            for (Pipe p : pipes) if (p.fut != null) p.fut.orTimeout(10, java.util.concurrent.TimeUnit.SECONDS).join();
        } catch (Exception ignored) {
        }
        client.close();
    }
}
