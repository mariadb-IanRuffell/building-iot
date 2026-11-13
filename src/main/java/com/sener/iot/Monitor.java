package com.sener.iot;

import org.apache.ignite.table.Tuple;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Housekeeping (rate sampling, stats persistence, retention) and construction of the live state snapshot. */
public final class Monitor implements AutoCloseable {
    private final Config cfg;
    private final IngestPipeline pipeline;
    private final GridGainStore store;
    private final SimClock clock;
    private final ScheduledExecutorService ses = Executors.newScheduledThreadPool(2);

    private volatile long rows = -1;
    private volatile long diskRows = -1;
    private volatile List<String> nodes = List.of();
    private volatile boolean connected;
    private volatile String lastJson = "{}";
    private volatile long seq;

    public Monitor(Config cfg, IngestPipeline pipeline, GridGainStore store, SimClock clock) {
        this.cfg = cfg;
        this.pipeline = pipeline;
        this.store = store;
        this.clock = clock;
    }

    public void start() {
        ses.scheduleAtFixedRate(() -> guard(() -> pipeline.stats().values().forEach(StreamStats::sample)), 1, 1, TimeUnit.SECONDS);
        ses.scheduleAtFixedRate(() -> guard(() -> {
            lastJson = Json.write(snapshot());
            seq++;
        }), 1, 1, TimeUnit.SECONDS);
        ses.scheduleAtFixedRate(() -> guard(this::persistStats), 10, 10, TimeUnit.SECONDS);
        ses.scheduleAtFixedRate(() -> guard(() -> {
            rows = store.hotRowCount();
            nodes = store.connectedNodes();
            connected = !nodes.isEmpty();
        }), 0, 15, TimeUnit.SECONDS);
        ses.scheduleAtFixedRate(() -> guard(() -> store.purgeHotOlderThan(Instant.now().minusSeconds(cfg.retentionHours() * 3600L))),
                60, 60, TimeUnit.SECONDS);
        ses.scheduleAtFixedRate(() -> guard(() -> diskRows = store.diskRowCount()), 0, 60, TimeUnit.SECONDS);
        // cheap liveness check for the cluster connection
        ses.scheduleAtFixedRate(() -> guard(() -> {
            nodes = store.connectedNodes();
            connected = !nodes.isEmpty();
        }), 2, 2, TimeUnit.SECONDS);
    }

    private void guard(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            store.lastError = String.valueOf(t);
            System.err.println("[monitor] " + t);
        }
    }

    private void persistStats() {
        Instant now = Instant.now();
        List<Tuple> rowsOut = new ArrayList<>();
        for (StreamStats st : pipeline.stats().values()) {
            rowsOut.add(Tuple.create().set("STREAM_TYPE", st.type.name()).set("TS", now)
                    .set("RECEIVED", st.deltaSincePersist()).set("RATE_PER_SEC", st.rate(10))
                    .set("REJECTED", st.rejected.sum()));
        }
        store.saveStats(rowsOut);
    }

    public String json() {
        return lastJson;
    }

    public long seq() {
        return seq;
    }

    public Map<String, Object> snapshot() {
        long now = System.currentTimeMillis();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("now", now);
        m.put("uptimeSec", (now - pipeline.startedMs) / 1000);

        Map<String, Object> sim = new LinkedHashMap<>();
        sim.put("time", clock.hhmm());
        sim.put("hour", clock.hourOfDay());
        sim.put("day", clock.dayNumber());
        m.put("sim", sim);

        Map<String, Object> cl = new LinkedHashMap<>();
        cl.put("connected", connected);
        cl.put("nodes", nodes);
        cl.put("rows", rows);
        cl.put("diskRows", diskRows);
        cl.put("retentionHours", cfg.retentionHours());
        cl.put("streamers", store.streamersRunning());
        cl.put("queueLag", store.queueLag());
        cl.put("submitted", store.submitted.get());
        cl.put("dropped", store.dropped.get());
        cl.put("restarts", store.streamerRestarts.get());
        cl.put("lastError", store.lastError);
        m.put("cluster", cl);

        double rate1 = 0, rate10 = 0;
        List<Object> streams = new ArrayList<>();
        int sensorsTotal = 0, sensorsFresh = 0;
        for (StreamStats st : pipeline.stats().values()) {
            int total = 0, fresh = 0;
            for (SensorState ss : pipeline.states().values()) {
                if (ss.sensor.type() != st.type) continue;
                total++;
                if (ss.fresh(now)) fresh++;
            }
            sensorsTotal += total;
            sensorsFresh += fresh;
            Map<String, Object> o = new LinkedHashMap<>();
            o.put("type", st.type.name());
            o.put("label", st.type.label);
            o.put("sensors", total);
            o.put("fresh", fresh);
            String status = fresh == total ? "HEALTHY" : fresh == 0 ? (st.received.sum() == 0 ? "NO_DATA" : "DOWN") : "DEGRADED";
            o.put("status", status);
            double r1 = st.rate(1), r10 = st.rate(10);
            rate1 += r1;
            rate10 += r10;
            o.put("rate", r1);
            o.put("rate10", r10);
            o.put("rate60", st.rate(60));
            o.put("total", st.received.sum());
            o.put("rejected", st.rejected.sum());
            long last = st.lastRecvMs.get();
            o.put("lastAgeMs", last == 0 ? null : (Object) (now - last));
            o.put("history", st.history(120));
            streams.add(o);
        }
        m.put("streams", streams);

        Map<String, Object> tot = new LinkedHashMap<>();
        tot.put("received", pipeline.totalReceived());
        tot.put("rejected", pipeline.totalRejected());
        tot.put("rate", rate1);
        tot.put("rate10", rate10);
        tot.put("sensors", sensorsTotal);
        tot.put("fresh", sensorsFresh);
        m.put("totals", tot);

        List<Object> sensors = new ArrayList<>();
        for (SensorState ss : pipeline.states().values()) {
            Sensor s = ss.sensor;
            Map<String, Object> o = new LinkedHashMap<>();
            o.put("id", s.id());
            o.put("name", s.name());
            o.put("type", s.type().name());
            o.put("floor", s.floor());
            o.put("zone", s.zone());
            o.put("unit", s.unit());
            o.put("v", ss.value);
            o.put("attr", ss.attr);
            o.put("age", ss.recvMs == 0 ? null : (Object) (now - ss.recvMs));
            o.put("n", ss.count.sum());
            o.put("fresh", ss.fresh(now));
            sensors.add(o);
        }
        m.put("sensorList", sensors);
        return m;
    }

    @Override
    public void close() {
        ses.shutdownNow();
    }
}
