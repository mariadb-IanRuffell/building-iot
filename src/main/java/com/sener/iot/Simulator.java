package com.sener.iot;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generates plausible building telemetry: outside weather drives indoor temperatures, working hours drive
 * occupancy, occupancy drives lights/CO2/power, and three lifts shuttle between floors.
 * Readings are delivered through a Sink - by default the app's own HTTP ingest endpoint, exactly like a real gateway.
 */
public final class Simulator implements Runnable {
    public interface Sink {
        void send(List<Reading> batch) throws Exception;
    }

    /** POSTs batches to the ingest API. */
    public static Sink httpSink(String baseUrl) {
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        return batch -> {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < batch.size(); i++) {
                Reading r = batch.get(i);
                if (i > 0) sb.append(',');
                sb.append("{\"sensorId\":").append(Json.write(r.sensorId()))
                  .append(",\"ts\":").append(r.tsMs())
                  .append(",\"value\":").append(Json.write(r.value()));
                if (r.attr() != null) sb.append(",\"attr\":").append(Json.write(r.attr()));
                sb.append('}');
            }
            sb.append(']');
            HttpResponse<String> resp = http.send(HttpRequest.newBuilder(URI.create(baseUrl + "/api/ingest"))
                    .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(sb.toString())).build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 300) throw new IllegalStateException("ingest returned " + resp.statusCode() + " " + resp.body());
        };
    }

    private static final double[][] OCC_PROFILE = {
        {0, .02}, {6.5, .02}, {9, .9}, {12, .92}, {12.5, .6}, {13.5, .6}, {14, .9}, {17, .9}, {19, .08}, {24, .02}};
    private static final int[] CAPACITY = {30, 48, 48, 48, 48, 36};

    private static final class Lift {
        double floor;
        int dir;                  // -1 down, 0 idle, 1 up
        boolean doorsOpen;
        long doorUntil;
        int load;
        final java.util.ArrayDeque<Integer> queue = new java.util.ArrayDeque<>();
    }

    private final BuildingModel model;
    private final Sink sink;
    private final SimClock clock;
    private final double scale;
    private final Random rnd = new Random();
    private final Map<String, Long> nextDue = new HashMap<>();
    private final Map<String, Long> silentUntil = new ConcurrentHashMap<>();   // by sensor id or stream type name
    private final Map<String, Boolean> forced = new HashMap<>();

    // physical state
    private double outTemp, outHum = 65, cloud = 0.3, wind = 5;
    private final double[] occ = new double[BuildingModel.FLOORS];
    private final double[] bias = new double[BuildingModel.FLOORS];
    private final double[][] temp = new double[BuildingModel.FLOORS][2];
    private final boolean[][] light = new boolean[BuildingModel.FLOORS][2];
    private final double[] hum = new double[BuildingModel.FLOORS];
    private final double[] co2 = new double[BuildingModel.FLOORS];
    private final Lift[] lifts = new Lift[BuildingModel.LIFTS];
    private double solar;
    private long lastTick = System.currentTimeMillis();
    private long lastFlush;
    private final List<Reading> batch = new ArrayList<>();
    private volatile long lastErrLog;

    public Simulator(BuildingModel model, Sink sink, SimClock clock, double scale) {
        this.model = model;
        this.sink = sink;
        this.clock = clock;
        this.scale = Math.max(0.1, scale);
        double h = clock.hourOfDay();
        outTemp = outTempTarget(h);
        for (int f = 0; f < BuildingModel.FLOORS; f++) {
            bias[f] = 0.8 + rnd.nextDouble() * 0.2;
            occ[f] = CAPACITY[f] * profile(h) * bias[f];
            hum[f] = 42;
            co2[f] = 430;
            temp[f][0] = temp[f][1] = 21.0 + rnd.nextDouble();
        }
        for (int l = 0; l < lifts.length; l++) {
            lifts[l] = new Lift();
            lifts[l].floor = rnd.nextInt(BuildingModel.FLOORS);
        }
    }

    /** Makes a whole stream (e.g. LIFT) or a single sensor go silent for the given number of seconds. */
    public void outage(String streamOrSensor, int seconds) {
        silentUntil.put(streamOrSensor, System.currentTimeMillis() + seconds * 1000L);
    }

    private static double profile(double h) {
        for (int i = 1; i < OCC_PROFILE.length; i++) {
            if (h <= OCC_PROFILE[i][0]) {
                double[] a = OCC_PROFILE[i - 1], b = OCC_PROFILE[i];
                return a[1] + (b[1] - a[1]) * (h - a[0]) / (b[0] - a[0]);
            }
        }
        return .02;
    }

    private static double outTempTarget(double h) {
        return 9 + 7 * Math.sin(2 * Math.PI * (h - 9) / 24);
    }

    private static double gauss(double x, double mu, double sd) {
        double z = (x - mu) / sd;
        return Math.exp(-0.5 * z * z);
    }

    @Override
    public void run() {
        try {
            tick();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void tick() {
        long now = System.currentTimeMillis();
        double dt = Math.min(1.0, (now - lastTick) / 1000.0);
        lastTick = now;
        double h = clock.hourOfDay();

        physics(h, dt, now);
        stepLifts(now, dt);

        // random single-sensor dropouts so the dashboard has something interesting to show
        if (rnd.nextInt((int) (1000 / Math.max(1, dt * 1000) * 90)) == 0) {
            List<Sensor> all = model.sensors();
            Sensor s = all.get(rnd.nextInt(all.size()));
            outage(s.id(), 20 + rnd.nextInt(25));
        }

        for (Sensor s : model.sensors()) {
            Long until = silentUntil.get(s.id());
            Long untilType = silentUntil.get(s.type().name());
            if ((until != null && now < until) || (untilType != null && now < untilType)) continue;
            long due = nextDue.getOrDefault(s.id(), 0L);
            boolean force = Boolean.TRUE.equals(forced.remove(s.id()));
            if (now < due && !force) continue;
            Reading r = sample(s, now);
            if (r != null) {
                batch.add(r);
                nextDue.put(s.id(), now + (long) (interval(s) / scale * (0.85 + rnd.nextDouble() * 0.3)));
            }
        }

        if (!batch.isEmpty() && now - lastFlush >= 400) {
            flush();
            lastFlush = now;
        }
    }

    private void flush() {
        List<Reading> copy = new ArrayList<>(batch);
        batch.clear();
        try {
            sink.send(copy);
        } catch (Exception e) {
            long n = System.currentTimeMillis();
            if (n - lastErrLog > 10_000) {
                lastErrLog = n;
                System.err.println("[simulator] delivery failed: " + e);
            }
        }
    }

    private long interval(Sensor s) {
        if (s.type() == StreamType.LIFT) {
            int idx = Integer.parseInt(s.id().substring(5)) - 1;
            Lift l = lifts[idx];
            return (l.dir != 0 || l.doorsOpen) ? 500 : s.type().intervalMs;
        }
        return s.type().intervalMs;
    }

    // ------------------------------------------------------------ physics

    private void physics(double h, double dt, long now) {
        double k = Math.min(1.0, dt);
        outTemp += (outTempTarget(h) - outTemp) * 0.02 * k + rnd.nextGaussian() * 0.02;
        cloud = Math.max(0, Math.min(1, cloud + rnd.nextGaussian() * 0.01));
        solar = h > 6 && h < 18 ? Math.max(0, 750 * Math.sin(Math.PI * (h - 6) / 12)) * (1 - 0.7 * cloud) : 0;
        outHum = Math.max(25, Math.min(99, outHum + (65 - 20 * Math.sin(2 * Math.PI * (h - 9) / 24) + 12 * cloud - outHum) * 0.02 * k + rnd.nextGaussian() * 0.1));
        wind = Math.max(0.5, Math.min(16, wind + rnd.nextGaussian() * 0.15 * k));

        double p = profile(h);
        for (int f = 0; f < BuildingModel.FLOORS; f++) {
            double target = CAPACITY[f] * p * bias[f];
            occ[f] += (target - occ[f]) * 0.04 * k + rnd.nextGaussian() * 0.25;
            occ[f] = Math.max(0, Math.min(CAPACITY[f], occ[f]));
            for (int z = 0; z < 2; z++) {
                double zocc = occ[f] / 2;
                // lights with hysteresis; flag a forced report on change
                boolean was = light[f][z];
                if (!was && zocc >= 2.5) light[f][z] = true;
                else if (was && zocc < 1.0) light[f][z] = false;
                if (was != light[f][z]) forced.put("LIGHT-F" + f + "-" + BuildingModel.ZONES[z], true);

                double gain = solar * 0.006 * gauss(h, z == 1 ? 9.5 : 15.5, 2.6);
                double raw = 21.5 + 0.06 * (outTemp - 21) + 0.035 * zocc + gain;
                double tgt = 21.5 + (raw - 21.5) * 0.85;
                temp[f][z] += (tgt - temp[f][z]) * 0.015 * k + rnd.nextGaussian() * 0.01;
            }
            hum[f] += (42 + 0.15 * (outHum - 60) - hum[f]) * 0.02 * k + rnd.nextGaussian() * 0.05;
            co2[f] += (430 + 14 * occ[f] - co2[f]) * 0.04 * k + rnd.nextGaussian() * 1.5;
        }
    }

    // -------------------------------------------------------------- lifts

    private void stepLifts(long now, double dt) {
        double totalOcc = 0;
        for (double o : occ) totalOcc += o;
        // call rate grows with occupancy: roughly 0.1 .. 1.5 calls/second
        double callsPerSec = 0.12 + totalOcc / 120.0;
        if (rnd.nextDouble() < callsPerSec * dt) {
            int floor = rnd.nextDouble() < 0.4 ? 0 : rnd.nextInt(BuildingModel.FLOORS);
            Lift best = null;
            double bestD = 1e9;
            for (Lift l : lifts) {
                double d = Math.abs(l.floor - floor) + l.queue.size() * 2;
                if (d < bestD) {
                    bestD = d;
                    best = l;
                }
            }
            if (best != null && !best.queue.contains(floor) && Math.round(best.floor) != floor) best.queue.add(floor);
        }
        for (int i = 0; i < lifts.length; i++) {
            Lift l = lifts[i];
            String id = "LIFT-" + (i + 1);
            if (l.doorsOpen) {
                if (now >= l.doorUntil) {
                    l.doorsOpen = false;
                    forced.put(id, true);
                }
                continue;
            }
            Integer target = l.queue.peek();
            if (target == null) {
                if (l.dir != 0) forced.put(id, true);
                l.dir = 0;
                continue;
            }
            int newDir = target > l.floor ? 1 : -1;
            if (newDir != l.dir) forced.put(id, true);
            l.dir = newDir;
            double step = 0.8 * dt;
            if (Math.abs(target - l.floor) <= step) {
                l.floor = target;
                l.queue.poll();
                l.doorsOpen = true;
                l.doorUntil = now + 3000;
                l.load = Math.max(0, Math.min(12, l.load + rnd.nextInt(5) - 2));
                forced.put(id, true);
            } else {
                l.floor += l.dir * step;
            }
        }
    }

    // ------------------------------------------------------------ sampling

    private Reading sample(Sensor s, long now) {
        String id = s.id();
        int f = s.floor();
        int z = "E".equals(s.zone()) ? 1 : 0;
        double v;
        String attr = null;
        switch (s.type()) {
            case TEMPERATURE: v = temp[f][z] + rnd.nextGaussian() * 0.02; break;
            case HUMIDITY: v = hum[f]; break;
            case LIGHTING: v = light[f][z] ? 1 : 0; break;
            case OCCUPANCY: v = Math.round(occ[f]); break;
            case AIR_QUALITY: v = Math.round(co2[f]); break;
            case POWER:
                if (f < 0) {
                    double dev = 0;
                    for (int ff = 0; ff < BuildingModel.FLOORS; ff++) dev += Math.abs(temp[ff][0] - 21.5) + Math.abs(temp[ff][1] - 21.5);
                    v = 12 + 2.4 * dev + 0.004 * solar + rnd.nextGaussian() * 0.3;
                } else {
                    int on = (light[f][0] ? 1 : 0) + (light[f][1] ? 1 : 0);
                    v = 2 + 0.09 * occ[f] + 1.4 * on + rnd.nextGaussian() * 0.15;
                }
                break;
            case LIFT: {
                Lift l = lifts[Integer.parseInt(id.substring(5)) - 1];
                v = Math.round(l.floor * 100) / 100.0;
                attr = "dir=" + (l.dir > 0 ? "UP" : l.dir < 0 ? "DOWN" : "IDLE") + ";door=" + (l.doorsOpen ? "OPEN" : "CLOSED") + ";load=" + l.load;
                break;
            }
            default:
                switch (id) {
                    case "OUT-TEMP": v = outTemp; break;
                    case "OUT-HUM": v = outHum; break;
                    case "OUT-SOLAR": v = solar; break;
                    default: v = wind;
                }
        }
        return new Reading(id, now, Math.round(v * 1000) / 1000.0, attr);
    }
}
