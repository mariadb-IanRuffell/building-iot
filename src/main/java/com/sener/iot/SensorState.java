package com.sener.iot;

import java.util.concurrent.atomic.LongAdder;

/** Live (in-memory) view of one sensor; GridGain is the system of record, this is the hot read cache. */
public final class SensorState {
    public final Sensor sensor;
    public volatile double value = Double.NaN;
    public volatile String attr;
    public volatile long tsMs;
    public volatile long recvMs;      // wall-clock time we last received something (0 = never in this run)
    public final LongAdder count = new LongAdder();

    public SensorState(Sensor sensor) {
        this.sensor = sensor;
    }

    public boolean fresh(long now) {
        return recvMs > 0 && now - recvMs <= sensor.staleAfterMs();
    }
}
