package com.sener.iot;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Energy-efficiency advisor.
 * 1. A rules engine analyses a recent window of readings aggregated by GridGain SQL and produces concrete findings
 *    with evidence and an estimated saving (always available, no external service).
 * 2. If ANTHROPIC_API_KEY is set, Claude is given the aggregated facts and findings and returns a ranking, short
 *    rationales and extra ideas. Only aggregated building statistics are sent - no raw readings.
 */
public final class Advisor implements AutoCloseable {
    private static final double KW_PER_LIGHT_ZONE = 1.4;     // matches the power model of the simulated building

    private final BuildingModel model;
    private final GridGainStore store;
    private final IngestPipeline pipeline;
    private final SimClock clock;
    private final int windowSec;
    private final String apiKey = System.getenv("ANTHROPIC_API_KEY");
    private final String aiModel = System.getenv().getOrDefault("ANTHROPIC_MODEL", "claude-sonnet-5-5");
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();

    private volatile Map<String, Object> advice = Map.of("items", List.of(), "status", "starting");
    private volatile String aiSignature = "";
    private volatile long aiAt;
    private volatile Map<String, Object> aiResult;
    private volatile String aiError;
    private volatile long lastRefreshMs;

    public Advisor(Config cfg, BuildingModel model, GridGainStore store, IngestPipeline pipeline, SimClock clock) {
        this.model = model;
        this.store = store;
        this.pipeline = pipeline;
        this.clock = clock;
        // the demo day is compressed, so a short window already covers over an hour of simulated time
        this.windowSec = Integer.parseInt(System.getenv().getOrDefault("ADVISOR_WINDOW_SECONDS",
                cfg.simDayMinutes() < 120 ? "60" : "900"));
    }

    public void start() {
        ses.scheduleWithFixedDelay(() -> {
            try {
                analyse();
            } catch (Throwable t) {
                System.err.println("[advisor] " + t);
            }
        }, 20, 30, TimeUnit.SECONDS);
    }

    public Map<String, Object> advice() {
        return advice;
    }

    /** Re-runs the analysis now (rate limited to once per 5 s). */
    public synchronized Map<String, Object> refresh() {
        if (System.currentTimeMillis() - lastRefreshMs > 5000) {
            try {
                analyse();
            } catch (Throwable t) {
                System.err.println("[advisor] " + t);
            }
        }
        return advice;
    }

    public boolean aiEnabled() {
        return apiKey != null && !apiKey.isBlank();
    }

    // ------------------------------------------------------------------------ rules

    private static String fn(int f) {
        return BuildingModel.floorName(f);
    }

    private synchronized void analyse() {
        lastRefreshMs = System.currentTimeMillis();
        Map<String, double[]> a = store.aggregates(Instant.now().minusSeconds(windowSec));
        List<Finding> out = new ArrayList<>();
        if (a.size() < 20) {
            advice = Map.of("items", List.of(), "status", "collecting data", "windowSeconds", windowSec);
            return;
        }
        double outT = avg(a, "OUT-TEMP"), solar = avg(a, "OUT-SOLAR");
        double hvac = avg(a, "PWR-HVAC");
        double totalOcc = 0, totalPwr = Double.isNaN(hvac) ? 0 : hvac;
        for (int f = 0; f < BuildingModel.FLOORS; f++) {
            totalOcc += nz(avg(a, "OCC-F" + f));
            totalPwr += nz(avg(a, "PWR-F" + f));
        }

        for (int f = 0; f < BuildingModel.FLOORS; f++) {
            double occ = avg(a, "OCC-F" + f), co2 = avg(a, "CO2-F" + f);
            if (Double.isNaN(occ)) continue;
            // 1. lights burning in (nearly) empty floors
            for (String z : BuildingModel.ZONES) {
                double on = avg(a, "LIGHT-F" + f + "-" + z);
                if (!Double.isNaN(on) && on > 0.5 && occ < 3) {
                    out.add(new Finding("light-" + f + z, "medium", "Floor " + fn(f) + " " + (z.equals("W") ? "West" : "East"),
                            "Lights on in an almost empty zone",
                            String.format("Lights were on %.0f%% of the last %d s while the floor averaged only %.1f people.", on * 100, windowSec, occ),
                            "Switch the zone to presence-based control or add a timed auto-off (e.g. 10 min after last movement).",
                            KW_PER_LIGHT_ZONE * on, String.format("LIGHT-F%d-%s avg %.2f, OCC-F%d avg %.1f", f, z, on, f, occ)));
                }
            }
            // 2. conditioning an unoccupied floor to the full comfort band
            double tw = avg(a, "TEMP-F" + f + "-W"), te = avg(a, "TEMP-F" + f + "-E");
            double tAvg = (nz(tw) + nz(te)) / 2;
            if (occ < 3 && tAvg > 20 && tAvg < 23.5) {
                out.add(new Finding("setback-" + f, "medium", "Floor " + fn(f), "Unoccupied floor held at comfort temperature",
                        String.format("Floor averaged %.1f people but is held at %.1f °C.", occ, tAvg),
                        "Apply an unoccupied setback (e.g. 18 °C heating / 27 °C cooling) with a pre-conditioning ramp before the next arrivals.",
                        0.8, String.format("OCC-F%d avg %.1f, TEMP avg %.1f °C", f, occ, tAvg)));
            }
            // 3. solar gain on a facade
            for (String z : BuildingModel.ZONES) {
                double t = avg(a, "TEMP-F" + f + "-" + z);
                if (!Double.isNaN(t) && t > 24 && solar > 300) {
                    out.add(new Finding("solar-" + f + z, "medium", "Floor " + fn(f) + " " + (z.equals("W") ? "West" : "East"),
                            "Solar heat gain driving cooling load",
                            String.format("Zone is at %.1f °C with %.0f W/m² of sunshine.", t, solar),
                            "Lower the blinds / enable solar-tracking shading on the " + (z.equals("W") ? "west" : "east") + " facade.",
                            Math.min(2.0, 0.5 * (t - 24) + 0.3), String.format("TEMP-F%d-%s avg %.1f °C, OUT-SOLAR avg %.0f", f, z, t, solar)));
                }
            }
            // 4. ventilation
            if (!Double.isNaN(co2)) {
                if (co2 > 950 && occ >= 5) {
                    out.add(new Finding("co2-high-" + f, "high", "Floor " + fn(f), "Air quality below target",
                            String.format("CO₂ averaged %.0f ppm with %.0f people on the floor.", co2, occ),
                            "Raise the fresh-air rate for this floor (demand-controlled ventilation) - a productivity and health issue, not an energy saving.",
                            0, String.format("CO2-F%d avg %.0f ppm", f, co2)));
                } else if (co2 < 480 && occ < 3) {
                    out.add(new Finding("vent-" + f, "low", "Floor " + fn(f), "Over-ventilating an empty floor",
                            String.format("CO₂ is only %.0f ppm (close to outdoor air) with %.1f people.", co2, occ),
                            "Reduce the minimum fresh-air rate while the floor is unoccupied (CO₂-based demand-controlled ventilation).",
                            0.5, String.format("CO2-F%d avg %.0f, OCC-F%d avg %.1f", f, co2, f, occ)));
                }
            }
        }

        // 5. free cooling
        double inAvg = 0;
        int n = 0;
        for (Sensor s : model.sensors()) if (s.type() == StreamType.TEMPERATURE && !Double.isNaN(avg(a, s.id()))) { inAvg += avg(a, s.id()); n++; }
        inAvg = n == 0 ? Double.NaN : inAvg / n;
        if (!Double.isNaN(hvac) && !Double.isNaN(outT) && outT >= 8 && outT <= 17 && inAvg > 22.3 && hvac > 15) {
            out.add(new Finding("freecool", "high", "Roof HVAC plant", "Mechanical cooling while outside air is cool enough",
                    String.format("Outside is %.1f °C, the building averages %.1f °C, yet the HVAC plant draws %.1f kW.", outT, inAvg, hvac),
                    "Enable the air-side economiser (free cooling) and reduce chiller duty.", 0.15 * hvac,
                    String.format("OUT-TEMP avg %.1f, PWR-HVAC avg %.1f kW", outT, hvac)));
        }

        // 6. lifts
        int active = 0;
        for (int l = 1; l <= BuildingModel.LIFTS; l++) {
            double[] x = a.get("LIFT-" + l);
            if (x != null && x[3] > 0 && (x[2] - x[1]) > 0.5) active++;
        }
        if (totalOcc < 45 && active >= 2) {
            out.add(new Finding("lifts", "low", "Lift core", "More lifts in service than demand needs",
                    String.format("%d of 3 lifts moved while the whole building averaged only %.0f people.", active, totalOcc),
                    "Park all but one lift (standby / low-power mode) during low-traffic periods and use group dispatching.",
                    0.5 * (active - 1), String.format("%d lifts active, total people %.0f", active, totalOcc)));
        }

        // 7. baseload
        if (totalOcc < 8 && totalPwr > 40) {
            out.add(new Finding("baseload", "high", "Whole building", "High baseload with almost nobody present",
                    String.format("Only %.0f people but %.1f kW being drawn.", totalOcc, totalPwr),
                    "Audit what is running out of hours: plant schedules, lighting, plug loads. Compare per-floor power to find the largest consumer.",
                    Math.max(0, totalPwr - 30) * 0.5, String.format("total power %.1f kW, people %.0f", totalPwr, totalOcc)));
        }

        // 8. data quality
        long silent = pipeline.states().values().stream().filter(s -> s.recvMs > 0 && !s.fresh(System.currentTimeMillis())).count();
        if (silent > 0) {
            out.add(new Finding("silent", "info", "Data quality", silent + " sensor(s) not reporting",
                    "Recommendations for the affected areas may be incomplete.", "Check the Stream monitor page for the silent sensors.", 0, silent + " silent"));
        }

        out.sort(Comparator.comparingInt((Finding x) -> sevRank(x.severity())).thenComparing(x -> -x.savingKw()));
        double saving = out.stream().mapToDouble(Finding::savingKw).sum();

        Map<String, Object> res = new LinkedHashMap<>();
        res.put("status", "ok");
        res.put("generatedAt", System.currentTimeMillis());
        res.put("windowSeconds", windowSec);
        res.put("simTime", clock.hhmm());
        res.put("source", "rules");
        res.put("aiEnabled", aiEnabled());
        res.put("currentPowerKw", totalPwr);
        res.put("totalSavingKw", saving);
        res.put("savingPct", totalPwr > 0 ? saving / totalPwr * 100 : 0);
        List<Map<String, Object>> items = new ArrayList<>();
        for (Finding f : out) items.add(f.toMap());
        res.put("items", items);
        res.put("summary", out.isEmpty()
                ? "No inefficiencies detected in the last " + windowSec + " s - lighting, comfort and ventilation match occupancy."
                : String.format("%d suggestion%s with an estimated saving of up to %.1f kW (%.0f%% of the %.0f kW currently drawn).",
                        out.size(), out.size() == 1 ? "" : "s", saving, totalPwr > 0 ? saving / totalPwr * 100 : 0, totalPwr));

        if (aiEnabled() && !out.isEmpty()) enrichWithClaude(res, out, a, outT, solar, inAvg, totalOcc, totalPwr);
        else if (!aiEnabled()) res.put("aiNote", "AI narrative is off: set ANTHROPIC_API_KEY and restart to have Claude rank and explain these findings.");
        advice = res;
    }

    // ------------------------------------------------------------------------ Claude

    @SuppressWarnings("unchecked")
    private void enrichWithClaude(Map<String, Object> res, List<Finding> findings, Map<String, double[]> a,
                                  double outT, double solar, double inAvg, double occ, double pwr) {
        String sig = findings.stream().map(Finding::id).sorted().reduce("", (x, y) -> x + "|" + y);
        long now = System.currentTimeMillis();
        if (!(sig.equals(aiSignature) && now - aiAt < 600_000) || aiResult == null) {
            try {
                Map<String, Object> facts = new LinkedHashMap<>();
                facts.put("simulatedTime", clock.hhmm());
                facts.put("windowSeconds", windowSec);
                facts.put("outsideTempC", round(outT));
                facts.put("solarWm2", round(solar));
                facts.put("avgIndoorTempC", round(inAvg));
                facts.put("peopleInBuilding", round(occ));
                facts.put("totalPowerKw", round(pwr));
                List<Object> fl = new ArrayList<>();
                for (Finding f : findings) fl.add(f.toMap());
                facts.put("findings", fl);
                String system = "You are a building energy-efficiency advisor for a six-storey office building. You receive aggregated sensor statistics and "
                        + "findings from a rules engine. Respond ONLY with JSON: {\"summary\": string (max 60 words, plain English for a facilities manager), "
                        + "\"ranking\": [{\"id\": finding id, \"rank\": 1-based integer, \"why\": string max 25 words}], \"ideas\": [up to 3 extra ideas strictly grounded in the data]}. "
                        + "Rank by saving, effort and comfort risk. Never invent numbers that are not in the input.";
                String body = Json.write(Map.of("model", aiModel, "max_tokens", 1500, "system", system,
                        "messages", List.of(Map.of("role", "user", "content", Json.write(facts)))));
                HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create("https://api.anthropic.com/v1/messages"))
                        .timeout(Duration.ofSeconds(60)).header("x-api-key", apiKey).header("anthropic-version", "2023-06-01")
                        .header("content-type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() != 200) throw new IllegalStateException("Anthropic API returned " + r.statusCode());
                Map<String, Object> msg = (Map<String, Object>) Json.parse(r.body());
                String text = (String) ((Map<String, Object>) ((List<Object>) msg.get("content")).get(0)).get("text");
                text = text.trim();
                if (text.startsWith("```")) text = text.replaceAll("^```[a-zA-Z]*\\s*", "").replaceAll("```\\s*$", "");
                aiResult = (Map<String, Object>) Json.parse(text);
                aiSignature = sig;
                aiAt = now;
                aiError = null;
            } catch (Exception e) {
                aiError = String.valueOf(e);
                System.err.println("[advisor] Claude call failed: " + e);
            }
        }
        if (aiError != null) res.put("aiNote", "Claude analysis unavailable (" + aiError + ") - showing rules-engine results only.");
        Map<String, Object> ai = aiResult;
        if (ai == null) return;
        res.put("source", "rules+claude");
        res.put("aiModel", aiModel);
        if (ai.get("summary") instanceof String s) res.put("summary", s);
        res.put("ideas", ai.get("ideas") instanceof List<?> l ? l : List.of());
        Map<String, Object[]> rank = new LinkedHashMap<>();
        if (ai.get("ranking") instanceof List<?> rl) {
            for (Object o : rl) {
                if (o instanceof Map<?, ?> m && m.get("id") instanceof String id && m.get("rank") instanceof Number n)
                    rank.put(id, new Object[]{n.intValue(), m.get("why")});
            }
        }
        List<Map<String, Object>> items = (List<Map<String, Object>>) res.get("items");
        for (Map<String, Object> it : items) {
            Object[] rk = rank.get(it.get("id"));
            if (rk != null) {
                it.put("rank", rk[0]);
                it.put("aiWhy", rk[1]);
            }
        }
        items.sort(Comparator.comparingInt(it -> it.get("rank") instanceof Integer i ? i : 999));
    }

    // ------------------------------------------------------------------------ helpers

    private static double avg(Map<String, double[]> a, String id) {
        double[] x = a.get(id);
        return x == null ? Double.NaN : x[0];
    }

    private static double nz(double v) {
        return Double.isNaN(v) ? 0 : v;
    }

    private static double round(double v) {
        return Double.isNaN(v) ? 0 : Math.round(v * 10) / 10.0;
    }

    private static int sevRank(String s) {
        return switch (s) { case "high" -> 0; case "medium" -> 1; case "low" -> 2; default -> 3; };
    }

    public record Finding(String id, String severity, String area, String title, String detail, String action,
                          double savingKw, String evidence) {
        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", id);
            m.put("severity", severity);
            m.put("area", area);
            m.put("title", title);
            m.put("detail", detail);
            m.put("action", action);
            m.put("savingKw", Math.round(savingKw * 100) / 100.0);
            m.put("evidence", evidence);
            return m;
        }
    }

    @Override
    public void close() {
        ses.shutdownNow();
    }
}
