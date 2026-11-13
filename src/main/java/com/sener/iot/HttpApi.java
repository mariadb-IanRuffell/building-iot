package com.sener.iot;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/** HTTP front end: ingest endpoint, live SSE stream, trend/history APIs and the two web pages. */
public final class HttpApi implements AutoCloseable {
    private final HttpServer server;
    private final IngestPipeline pipeline;
    private final GridGainStore store;
    private final Monitor monitor;
    private final BuildingModel model;
    private volatile Simulator simulator;

    public HttpApi(int port, BuildingModel model, IngestPipeline pipeline, GridGainStore store, Monitor monitor) throws IOException {
        this.model = model;
        this.pipeline = pipeline;
        this.store = store;
        this.monitor = monitor;
        server = HttpServer.create(new InetSocketAddress(port), 512);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/", this::staticFile);
        server.createContext("/api/ingest", this::ingest);
        server.createContext("/api/live", this::live);
        server.createContext("/api/state", ex -> json(ex, 200, monitor.json()));
        server.createContext("/api/trends", this::trends);
        server.createContext("/api/history", this::history);
        server.createContext("/api/sensors", this::sensors);
        server.createContext("/api/sim/outage", this::outage);
    }

    public void setSimulator(Simulator s) {
        this.simulator = s;
    }

    public void start() {
        server.start();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    // ------------------------------------------------------------------ ingest

    @SuppressWarnings("unchecked")
    private void ingest(HttpExchange ex) throws IOException {
        try {
            if (!"POST".equals(ex.getRequestMethod())) {
                json(ex, 405, "{\"error\":\"POST a JSON reading or array of readings\"}");
                return;
            }
            Object body = Json.parse(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            List<Object> items = new ArrayList<>();
            if (body instanceof List<?> l) items.addAll(l);
            else if (body instanceof Map<?, ?> m && m.get("readings") instanceof List<?> l) items.addAll(l);
            else items.add(body);

            int ok = 0;
            List<String> errors = new ArrayList<>();
            long now = System.currentTimeMillis();
            for (Object o : items) {
                if (!(o instanceof Map<?, ?> m0)) {
                    errors.add("reading must be an object");
                    pipeline.rejectedInvalid.incrementAndGet();
                    continue;
                }
                Map<String, Object> m = (Map<String, Object>) m0;
                String id = m.get("sensorId") instanceof String s ? s : null;
                double v = m.get("value") instanceof Number n ? n.doubleValue() : Double.NaN;
                long ts = now;
                if (m.get("ts") instanceof Number n) ts = n.longValue();
                else if (m.get("ts") instanceof String s) {
                    try {
                        ts = Instant.parse(s).toEpochMilli();
                    } catch (Exception e) {
                        errors.add("bad ts '" + s + "'");
                        pipeline.rejectedInvalid.incrementAndGet();
                        continue;
                    }
                }
                String attr = m.get("attr") instanceof String s ? s : null;
                String err = pipeline.accept(new Reading(id, ts, v, attr));
                if (err == null) ok++;
                else if (errors.size() < 5) errors.add(err);
            }
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("accepted", ok);
            resp.put("rejected", items.size() - ok);
            if (!errors.isEmpty()) resp.put("errors", errors);
            json(ex, items.size() - ok == 0 ? 200 : 207, Json.write(resp));
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            json(ex, 400, Json.write(Map.of("error", "invalid JSON: " + e.getMessage())));
        } catch (Exception e) {
            json(ex, 500, Json.write(Map.of("error", String.valueOf(e))));
        }
    }

    // --------------------------------------------------------------------- SSE

    private void live(HttpExchange ex) throws IOException {
        ex.getResponseHeaders().add("Content-Type", "text/event-stream");
        ex.getResponseHeaders().add("Cache-Control", "no-cache");
        ex.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        ex.sendResponseHeaders(200, 0);
        try (OutputStream os = ex.getResponseBody()) {
            long seen = -1;
            long lastWrite = 0;
            while (true) {
                long s = monitor.seq();
                long now = System.currentTimeMillis();
                if (s != seen) {
                    os.write(("data: " + monitor.json() + "\n\n").getBytes(StandardCharsets.UTF_8));
                    os.flush();
                    seen = s;
                    lastWrite = now;
                } else if (now - lastWrite > 15_000) {
                    os.write(": keepalive\n\n".getBytes(StandardCharsets.UTF_8));
                    os.flush();
                    lastWrite = now;
                }
                Thread.sleep(100);
            }
        } catch (IOException | InterruptedException e) {
            // client went away
        }
    }

    // ------------------------------------------------------------------ queries

    private void trends(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex);
        int minutes = Math.max(1, Math.min(60, parseInt(q.get("minutes"), 30)));
        int points = minutes * 6;
        long step = TrendStore.STEP_MS;
        long end = System.currentTimeMillis();
        end -= Math.floorMod(end, step);
        long from = end - (points - 1) * step;
        List<String> ids = new ArrayList<>();
        for (String p : q.getOrDefault("ids", "").split(",")) {
            p = p.trim();
            if (p.isEmpty()) continue;
            if (p.endsWith("*")) {
                String prefix = p.substring(0, p.length() - 1);
                for (Sensor s : model.sensors()) if (s.id().startsWith(prefix)) ids.add(s.id());
            } else if (model.get(p) != null) {
                ids.add(p);
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("from", from);
        out.put("step", step);
        out.put("points", points);
        Map<String, Object> series = new LinkedHashMap<>();
        for (String id : ids) series.put(id, pipeline.trends.aligned(id, from, points));
        out.put("series", series);
        json(ex, 200, Json.write(out));
    }

    private void history(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex);
        String id = q.get("sensorId");
        Sensor s = id == null ? null : model.get(id);
        if (s == null) {
            json(ex, 404, "{\"error\":\"unknown sensorId\"}");
            return;
        }
        int minutes = Math.max(1, Math.min(cfgMaxMinutes(), parseInt(q.get("minutes"), 60)));
        try {
            long t0 = System.nanoTime();
            List<double[]> pts = store.history(id, Instant.now().minusSeconds(minutes * 60L), parseInt(q.get("points"), 300));
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("sensorId", id);
            out.put("name", s.name());
            out.put("unit", s.unit());
            out.put("type", s.type().name());
            out.put("minutes", minutes);
            out.put("queryMs", (System.nanoTime() - t0) / 1_000_000);
            out.put("points", pts);   // [ts, avg, min, max]
            json(ex, 200, Json.write(out));
        } catch (Exception e) {
            json(ex, 500, Json.write(Map.of("error", String.valueOf(e))));
        }
    }

    private int cfgMaxMinutes() {
        return 24 * 60;
    }

    private void sensors(HttpExchange ex) throws IOException {
        List<Object> l = new ArrayList<>();
        for (Sensor s : model.sensors()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", s.id());
            m.put("name", s.name());
            m.put("type", s.type().name());
            m.put("floor", s.floor());
            m.put("zone", s.zone());
            m.put("unit", s.unit());
            l.add(m);
        }
        json(ex, 200, Json.write(l));
    }

    private void outage(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex);
        Simulator sim = simulator;
        if (sim == null) {
            json(ex, 409, "{\"error\":\"simulator not running in this process\"}");
            return;
        }
        String target = q.getOrDefault("target", "");
        int secs = Math.max(1, Math.min(600, parseInt(q.get("seconds"), 30)));
        sim.outage(target, secs);
        json(ex, 200, Json.write(Map.of("target", target, "seconds", secs)));
    }

    // ------------------------------------------------------------------ plumbing

    private void staticFile(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        if (path.equals("/")) path = "/dashboard.html";
        else if (path.equals("/building")) path = "/building.html";
        if (path.contains("..")) {
            json(ex, 400, "{}");
            return;
        }
        try (InputStream in = HttpApi.class.getResourceAsStream("/web" + path)) {
            if (in == null) {
                json(ex, 404, "{\"error\":\"not found\"}");
                return;
            }
            byte[] b = in.readAllBytes();
            String ct = path.endsWith(".html") ? "text/html; charset=utf-8"
                    : path.endsWith(".css") ? "text/css; charset=utf-8"
                    : path.endsWith(".js") ? "text/javascript; charset=utf-8"
                    : path.endsWith(".svg") ? "image/svg+xml" : "application/octet-stream";
            ex.getResponseHeaders().add("Content-Type", ct);
            ex.getResponseHeaders().add("Cache-Control", "no-cache");
            ex.sendResponseHeaders(200, b.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(b);
            }
        }
    }

    private static void json(HttpExchange ex, int code, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        ex.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> m = new LinkedHashMap<>();
        String q = ex.getRequestURI().getRawQuery();
        if (q == null) return m;
        for (String kv : q.split("&")) {
            int i = kv.indexOf('=');
            String k = i < 0 ? kv : kv.substring(0, i);
            String v = i < 0 ? "" : kv.substring(i + 1);
            m.put(URLDecoder.decode(k, StandardCharsets.UTF_8), URLDecoder.decode(v, StandardCharsets.UTF_8));
        }
        return m;
    }

    private static int parseInt(String s, int d) {
        try {
            return s == null ? d : Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return d;
        }
    }
}
