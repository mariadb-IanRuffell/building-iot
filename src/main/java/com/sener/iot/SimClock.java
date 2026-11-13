package com.sener.iot;

/** Simulated time of day so a full working day can be watched in minutes (SIM_DAY_MINUTES, default 20). */
public final class SimClock {
    private final long startReal = System.currentTimeMillis();
    private final double dayMs;
    private static final double START_HOUR = 7.5;     // the demo day begins at 07:30

    public SimClock(double dayMinutes) {
        this.dayMs = dayMinutes * 60_000.0;
    }

    private double simHoursSinceStart() {
        return (System.currentTimeMillis() - startReal) / dayMs * 24.0;
    }

    public double hourOfDay() {
        return (START_HOUR + simHoursSinceStart()) % 24.0;
    }

    public int dayNumber() {
        return 1 + (int) ((START_HOUR + simHoursSinceStart()) / 24.0);
    }

    public String hhmm() {
        double h = hourOfDay();
        int hh = (int) h;
        int mm = (int) ((h - hh) * 60);
        return String.format("%02d:%02d", hh, mm);
    }
}
