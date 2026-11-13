# Building IoT on GridGain 9.2

A Java 21 application that receives sensor telemetry from an office building (temperatures, lights,
lifts, occupancy, CO₂, power, outside weather), stores it in **GridGain 9.2**, and serves two live web pages:

| Page | URL | What it shows |
|---|---|---|
| **Stream monitor** | <http://localhost:8080/> | Receive rate per stream, stream health (Healthy / Degraded / Down), per-sensor status and last value, rejects, GridGain cluster and Data Streamer state. Click a sensor for its stored history. |
| **Building overview** | <http://localhost:8080/building> | Cross-section of a 6-storey tower: zone temperatures (colour), lights, people, CO₂, power per floor, three moving lifts, roof HVAC plant, sky/sun with outside weather. Trend panel (10/30/60 min) for the building or a clicked floor. Click anything for history. |

## Run it

```bash
scripts/start-cluster.sh     # 3-node GridGain 9.2 cluster in Docker + init (needs your license file; GG_NODES=1 for one node)
scripts/run.sh               # builds the jar if needed, starts server + embedded simulator
```

Then open the two URLs above. The simulator starts at 07:30 on a compressed day (20 real minutes = 24 h) so
occupancy, lights, temperatures and lift traffic visibly change; set `SIM_DAY_MINUTES=1440` for real time.

## Architecture

```
 sensors / gateways ──HTTP POST /api/ingest──►  IngestPipeline ──► live cache + trend ring + rate counters
 (embedded simulator                                │
  posts the same way)                               └─► GridGain Data Streamer (2 long-lived streamers)
                                                          ├─ SENSOR_READING  full time series
                                                          └─ SENSOR_LATEST   last value per sensor (upsert)
 browser ◄── SSE /api/live (1 Hz) ── Monitor        SQL ◄── /api/history  (history popups)
 browser ◄── /api/trends (in-memory 10 s buckets, re-seeded from GridGain on restart)
```

GridGain objects (created on start-up by `GridGainStore.createSchema()` with SQL DDL):

| Object | Purpose |
|---|---|
| zone `IOT_ZONE` | 16 partitions, 3 replicas via `run.sh` (`GG_PARTITIONS`, `GG_REPLICAS`; the app also ALTERs an existing zone to match) |
| `SENSOR` | sensor registry |
| `SENSOR_READING (SENSOR_ID, TS, VAL, ATTR)` | time series, `COLOCATE BY (SENSOR_ID)`, sorted indexes on `(SENSOR_ID, TS)` and `TS` |
| `SENSOR_LATEST` | latest value per sensor |
| `INGEST_STATS` | per-stream receive rate persisted every 10 s |

Retention: readings older than `RETENTION_HOURS` (default 6) are purged every minute with a SQL `DELETE`.

## Ingest API

```bash
curl -X POST http://localhost:8080/api/ingest -H 'Content-Type: application/json' \
  -d '[{"sensorId":"TEMP-F2-W","value":23.4},
       {"sensorId":"LIFT-1","value":3.0,"attr":"dir=UP;door=CLOSED;load=4","ts":"2026-11-13T09:30:00Z"}]'
```

`ts` is optional (epoch ms or ISO-8601, default = receive time). Unknown sensors and non-numeric values are
rejected and counted (response `207` lists reasons). Sensor IDs: `GET /api/sensors`.

To run the simulator as a separate process (a stand-in for real gateways): `java -jar target/building-iot.jar simulate`
(`INGEST_URL` defaults to `http://localhost:8080`; set `SIM_ENABLED=false` on the server).

## Configuration (environment variables)

| Variable | Default | |
|---|---|---|
| `GG_ADDRESSES` | `127.0.0.1:10800` (`run.sh`: all three nodes) | comma-separated GridGain client endpoints |
| `HTTP_PORT` | `8080` | |
| `SIM_ENABLED` / `SIM_MODE` | `true` / `http` | `direct` bypasses HTTP and calls the pipeline |
| `SIM_SCALE` | `1.0` | message-rate multiplier (≈22 msg/s at 1.0; try 20 for load) |
| `SIM_DAY_MINUTES` | `20` | real minutes per simulated day |
| `RETENTION_HOURS` | `6` | |

Demo helper: the **Simulate outage** button on each stream card (or `POST /api/sim/outage?target=LIFT&seconds=30`)
silences a stream so you can watch it go Degraded/Down and recover. The simulator also randomly silences single
sensors for 20–45 s every ~90 s.

## Notes

* Client dependency: `org.gridgain:ignite-client:9.2.0-b1` from the GridGain Maven repository (declared in `pom.xml`).
* The 9.2.0-b1 Docker image crashes at start-up on Apple-Silicon Docker (native columnar library). `start-cluster.sh`
  removes the unused columnar jar inside the container as a workaround; on other hosts you can run the stock image.
* Cluster init needs a GridGain license (`GG_LICENSE`, default `~/gridgain/gridgain-license.json`). It is never copied into this project.
* Three Docker nodes on one host, no TLS/auth - a development setup. Enable authentication/TLS and put the ingest
  endpoint behind your gateway for real use.
