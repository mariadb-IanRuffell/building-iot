package com.sener.iot;

/** Static description of one sensor. floor = -1 for building-level / outside sensors. */
public record Sensor(String id, String name, StreamType type, int floor, String zone, String unit) {
    public long staleAfterMs() {
        return type.intervalMs * 3 + 2000;
    }
}
