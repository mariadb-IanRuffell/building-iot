package com.sener.iot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A six-storey (G + 5) office building with two zones (West/East) per floor, three lifts and a roof plant. */
public final class BuildingModel {
    public static final int FLOORS = 6;
    public static final int LIFTS = 3;
    public static final String[] ZONES = {"W", "E"};

    private final Map<String, Sensor> byId = new LinkedHashMap<>();

    public BuildingModel() {
        for (int f = 0; f < FLOORS; f++) {
            String fl = floorName(f);
            for (String z : ZONES) {
                String side = z.equals("W") ? "West" : "East";
                add(new Sensor("TEMP-F" + f + "-" + z, "Floor " + fl + " " + side + " temperature", StreamType.TEMPERATURE, f, z, "°C"));
                add(new Sensor("LIGHT-F" + f + "-" + z, "Floor " + fl + " " + side + " lights", StreamType.LIGHTING, f, z, "on/off"));
            }
            add(new Sensor("HUM-F" + f, "Floor " + fl + " humidity", StreamType.HUMIDITY, f, "", "%RH"));
            add(new Sensor("OCC-F" + f, "Floor " + fl + " occupancy", StreamType.OCCUPANCY, f, "", "people"));
            add(new Sensor("CO2-F" + f, "Floor " + fl + " CO2", StreamType.AIR_QUALITY, f, "", "ppm"));
            add(new Sensor("PWR-F" + f, "Floor " + fl + " power", StreamType.POWER, f, "", "kW"));
        }
        add(new Sensor("PWR-HVAC", "Roof HVAC plant power", StreamType.POWER, -1, "", "kW"));
        for (int l = 1; l <= LIFTS; l++) {
            add(new Sensor("LIFT-" + l, "Lift " + l, StreamType.LIFT, -1, "", "floor"));
        }
        add(new Sensor("OUT-TEMP", "Outside temperature", StreamType.WEATHER, -1, "", "°C"));
        add(new Sensor("OUT-HUM", "Outside humidity", StreamType.WEATHER, -1, "", "%RH"));
        add(new Sensor("OUT-SOLAR", "Solar irradiance", StreamType.WEATHER, -1, "", "W/m²"));
        add(new Sensor("OUT-WIND", "Wind speed", StreamType.WEATHER, -1, "", "m/s"));
    }

    private void add(Sensor s) {
        byId.put(s.id(), s);
    }

    public static String floorName(int f) {
        return f == 0 ? "G" : Integer.toString(f);
    }

    public Sensor get(String id) {
        return byId.get(id);
    }

    public List<Sensor> sensors() {
        return Collections.unmodifiableList(new ArrayList<>(byId.values()));
    }
}
