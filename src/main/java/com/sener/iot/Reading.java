package com.sener.iot;

/** One measurement. attr carries optional extra state (e.g. lift direction/doors/load). */
public record Reading(String sensorId, long tsMs, double value, String attr) {
}
