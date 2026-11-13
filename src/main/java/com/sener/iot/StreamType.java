package com.sener.iot;

/** The logical data streams (one per kind of sensor) shown on the monitoring dashboard. */
public enum StreamType {
    TEMPERATURE("Temperature", 2000),
    HUMIDITY("Humidity", 5000),
    LIGHTING("Lighting", 5000),
    OCCUPANCY("Occupancy", 3000),
    AIR_QUALITY("Air quality (CO2)", 5000),
    POWER("Power", 1000),
    LIFT("Lifts", 5000),
    WEATHER("Outside weather", 5000);

    public final String label;
    /** Nominal (slowest) reporting interval; a sensor is "stale" after 3x this plus a grace period. */
    public final long intervalMs;

    StreamType(String label, long intervalMs) {
        this.label = label;
        this.intervalMs = intervalMs;
    }
}
