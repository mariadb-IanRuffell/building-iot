package com.sener.iot;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Entry point for every reading, whatever its source (HTTP, simulator, ...):
 * validate -> update live cache/trends/statistics -> hand to the GridGain Data Streamer.
 */
public final class IngestPipeline {
    private final BuildingModel model;
    private final GridGainStore store;
    private final Map<String, SensorState> states = new LinkedHashMap<>();
    private final Map<StreamType, StreamStats> stats = new EnumMap<>(StreamType.class);
    public final TrendStore trends = new TrendStore();
    public final AtomicLong rejectedUnknown = new AtomicLong();
    public final AtomicLong rejectedInvalid = new AtomicLong();
    public final long startedMs = System.currentTimeMillis();

    public IngestPipeline(BuildingModel model, GridGainStore store) {
        this.model = model;
        this.store = store;
        for (Sensor s : model.sensors()) states.put(s.id(), new SensorState(s));
        for (StreamType t : StreamType.values()) stats.put(t, new StreamStats(t));
    }

    public Map<String, SensorState> states() {
        return states;
    }

    public Map<StreamType, StreamStats> stats() {
        return stats;
    }

    /** @return null if accepted, otherwise the rejection reason. */
    public String accept(Reading r) {
        Sensor s = r.sensorId() == null ? null : model.get(r.sensorId());
        if (s == null) {
            rejectedUnknown.incrementAndGet();
            return "unknown sensor '" + r.sensorId() + "'";
        }
        StreamStats st = stats.get(s.type());
        if (Double.isNaN(r.value()) || Double.isInfinite(r.value())) {
            st.rejected.increment();
            rejectedInvalid.incrementAndGet();
            return "invalid value for " + s.id();
        }
        long now = System.currentTimeMillis();
        SensorState ss = states.get(s.id());
        if (r.tsMs() >= ss.tsMs) {
            ss.value = r.value();
            ss.attr = r.attr();
            ss.tsMs = r.tsMs();
        }
        ss.recvMs = now;
        ss.count.increment();
        st.received.increment();
        st.lastRecvMs.set(now);
        trends.add(s.id(), r.tsMs(), r.value());
        store.write(s, r);
        return null;
    }

    /** Seeds the live cache from GridGain after a restart so the UI is never empty. */
    public void seed(GridGainStore.Latest l) {
        SensorState ss = states.get(l.id());
        if (ss == null) return;
        ss.value = l.value();
        ss.attr = l.attr();
        ss.tsMs = l.tsMs();
    }

    public long totalReceived() {
        long t = 0;
        for (StreamStats s : stats.values()) t += s.received.sum();
        return t;
    }

    public long totalRejected() {
        return rejectedUnknown.get() + rejectedInvalid.get();
    }
}
