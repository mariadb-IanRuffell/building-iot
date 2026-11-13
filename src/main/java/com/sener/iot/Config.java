package com.sener.iot;

import java.util.Arrays;
import java.util.List;

/** Runtime configuration, read from environment variables (all optional). */
public record Config(List<String> addresses, int httpPort, boolean simEnabled, boolean simViaHttp,
                     double simScale, double simDayMinutes, int retentionHours,
                     int partitions, int replicas) {

    public static Config load() {
        return new Config(
                Arrays.asList(env("GG_ADDRESSES", "127.0.0.1:10800").split("\\s*,\\s*")),
                Integer.parseInt(env("HTTP_PORT", "8080")),
                Boolean.parseBoolean(env("SIM_ENABLED", "true")),
                !"direct".equalsIgnoreCase(env("SIM_MODE", "http")),
                Double.parseDouble(env("SIM_SCALE", "1.0")),
                Double.parseDouble(env("SIM_DAY_MINUTES", "20")),
                Integer.parseInt(env("RETENTION_HOURS", "24")),
                Integer.parseInt(env("GG_PARTITIONS", "16")),
                Integer.parseInt(env("GG_REPLICAS", "1")));
    }

    private static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? d : v.trim();
    }
}
