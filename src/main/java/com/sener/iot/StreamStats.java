package com.sener.iot;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Receive counters and per-second rate history for one stream (sensor type). */
public final class StreamStats {
    public static final int HISTORY = 600;     // seconds of 1s samples kept

    public final StreamType type;
    public final LongAdder received = new LongAdder();
    public final LongAdder rejected = new LongAdder();
    public final AtomicLong lastRecvMs = new AtomicLong();

    private final long[] perSec = new long[HISTORY];
    private int head;           // next slot to write
    private int filled;
    private long prevTotal;
    private long prevPersisted;

    public StreamStats(StreamType type) {
        this.type = type;
    }

    /** Called once per second by the sampler thread. */
    public synchronized void sample() {
        long cur = received.sum();
        perSec[head] = cur - prevTotal;
        prevTotal = cur;
        head = (head + 1) % HISTORY;
        filled = Math.min(HISTORY, filled + 1);
    }

    /** Average msgs/sec over the last n samples. */
    public synchronized double rate(int n) {
        n = Math.min(n, filled);
        if (n == 0) return 0;
        long sum = 0;
        for (int i = 1; i <= n; i++) sum += perSec[(head - i + HISTORY) % HISTORY];
        return sum / (double) n;
    }

    /** Last n one-second samples, oldest first. */
    public synchronized double[] history(int n) {
        n = Math.min(n, filled);
        double[] out = new double[n];
        for (int i = 0; i < n; i++) out[i] = perSec[(head - n + i + HISTORY) % HISTORY];
        return out;
    }

    /** Messages received since the previous call (for the persisted stats table). */
    public synchronized long deltaSincePersist() {
        long cur = received.sum();
        long d = cur - prevPersisted;
        prevPersisted = cur;
        return d;
    }
}
