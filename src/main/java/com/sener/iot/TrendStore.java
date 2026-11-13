package com.sener.iot;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Rolling 60 minutes of 10-second averages per sensor, for sparklines/trends. Seeded from GridGain on start. */
public final class TrendStore {
    public static final long STEP_MS = 10_000;
    public static final int KEEP = 360;          // 60 min

    private static final class Series {
        final ArrayDeque<double[]> buckets = new ArrayDeque<>();   // {bucketStartMs, sum, count}
    }

    private final Map<String, Series> map = new ConcurrentHashMap<>();

    public void add(String id, long tsMs, double v) {
        if (Double.isNaN(v)) return;
        long b = tsMs - Math.floorMod(tsMs, STEP_MS);
        Series s = map.computeIfAbsent(id, k -> new Series());
        synchronized (s) {
            double[] last = s.buckets.peekLast();
            if (last != null && last[0] == b) {
                last[1] += v;
                last[2] += 1;
            } else if (last == null || b > last[0]) {
                s.buckets.addLast(new double[]{b, v, 1});
                while (s.buckets.size() > KEEP) s.buckets.pollFirst();
            }
            // out-of-order (older bucket) readings are ignored for trend purposes
        }
    }

    /** Returns averages aligned to the shared grid [fromBucket, fromBucket+STEP, ...] with NaN where missing. */
    public double[] aligned(String id, long fromBucket, int points) {
        double[] out = new double[points];
        java.util.Arrays.fill(out, Double.NaN);
        Series s = map.get(id);
        if (s == null) return out;
        synchronized (s) {
            for (double[] b : s.buckets) {
                int idx = (int) ((((long) b[0]) - fromBucket) / STEP_MS);
                if (idx >= 0 && idx < points) out[idx] = b[1] / b[2];
            }
        }
        return out;
    }
}
