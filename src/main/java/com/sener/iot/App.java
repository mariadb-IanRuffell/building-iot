package com.sener.iot;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Building IoT on GridGain 9.2.
 *
 *   java -jar building-iot.jar            run the ingest server + dashboards (+ embedded simulator)
 *   java -jar building-iot.jar simulate   run ONLY a sensor simulator that posts to an existing server
 *                                         (target taken from INGEST_URL, default http://localhost:8080)
 */
public final class App {
    public static void main(String[] args) throws Exception {
        Config cfg = Config.load();
        BuildingModel model = new BuildingModel();
        SimClock clock = new SimClock(cfg.simDayMinutes());

        if (args.length > 0 && args[0].equals("simulate")) {
            String url = System.getenv().getOrDefault("INGEST_URL", "http://localhost:" + cfg.httpPort());
            System.out.println("Simulating sensors -> " + url + "/api/ingest");
            ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
            ses.scheduleWithFixedDelay(new Simulator(model, Simulator.httpSink(url), clock, cfg.simScale()), 500, 100, TimeUnit.MILLISECONDS);
            Thread.currentThread().join();
            return;
        }

        System.out.println("Connecting to GridGain at " + cfg.addresses() + " ...");
        GridGainStore store = new GridGainStore(cfg);
        store.createSchema();
        store.registerSensors(model.sensors());
        System.out.println("Schema ready. Cluster nodes: " + store.connectedNodes());

        IngestPipeline pipeline = new IngestPipeline(model, store);
        store.loadLatest().forEach(pipeline::seed);
        long[] n = {0};
        store.scanSince(Instant.now().minusSeconds(3600), r -> {
            pipeline.trends.add(r.sensorId(), r.tsMs(), r.value());
            n[0]++;
        });
        System.out.println("Seeded trend cache from " + n[0] + " stored readings");
        store.startStreamers();

        Monitor monitor = new Monitor(cfg, pipeline, store, clock);
        monitor.start();
        HttpApi api = new HttpApi(cfg.httpPort(), model, pipeline, store, monitor);
        api.start();

        ScheduledExecutorService simExec = Executors.newSingleThreadScheduledExecutor();
        if (cfg.simEnabled()) {
            Simulator.Sink sink = cfg.simViaHttp()
                    ? Simulator.httpSink("http://localhost:" + cfg.httpPort())
                    : batch -> batch.forEach(pipeline::accept);
            Simulator sim = new Simulator(model, sink, clock, cfg.simScale());
            api.setSimulator(sim);
            simExec.scheduleWithFixedDelay(sim, 1000, 100, TimeUnit.MILLISECONDS);
            System.out.println("Embedded simulator running (" + (cfg.simViaHttp() ? "via HTTP ingest" : "direct") + ", scale " + cfg.simScale() + ")");
        }

        System.out.println();
        System.out.println("  Dashboard : http://localhost:" + cfg.httpPort() + "/");
        System.out.println("  Building  : http://localhost:" + cfg.httpPort() + "/building");
        System.out.println("  Ingest    : POST http://localhost:" + cfg.httpPort() + "/api/ingest");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            simExec.shutdownNow();
            api.close();
            monitor.close();
            store.close();
        }));
        Thread.currentThread().join();
    }
}
