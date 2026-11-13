# Building IoT on GridGain 9.2

A Java 21 application that receives sensor telemetry from an office building (temperatures, lights,
lifts, occupancy, CO₂, power, outside weather), stores it in **GridGain 9.2**, and serves two live web pages:

| Page | URL | What it shows |
|---|---|---|
| **Stream monitor** | <http://localhost:8080/> | Receive rate per stream, stream health (Healthy / Degraded / Down), per-sensor status and last value, rejects, GridGain cluster and Data Streamer state. Click a sensor for its stored history. |
| **Building overview** | <http://localhost:8080/building> | Cross-section of a 6-storey tower: zone temperatures (colour), lights, people, CO₂, power per floor, three moving lifts, roof HVAC plant, sky/sun with outside weather. Trend panel (10/30/60 min) for the building or a clicked floor. Click anything for history. |

| **Efficiency advisor** | <http://localhost:8080/advisor> | Suggestions for making the building more efficient, each with evidence, the action to take and an estimated kW saving. |

## Run it

```bash
scripts/start-cluster.sh     # 3-node GridGain 9.2 cluster in Docker + init (needs your license file; GG_NODES=1 for one node)
scripts/run-docker.sh        # builds the jar + image and runs the app as a container (http://localhost:8080)
# or, for development, without Docker:  scripts/run.sh
```

Then open the two URLs above. The simulator starts at 07:30 on a compressed day (20 real minutes = 24 h) so
occupancy, lights, temperatures and lift traffic visibly change; set `SIM_DAY_MINUTES=1440` for real time.

The app container (`building-iot-app`) and the GridGain containers share the Docker network `iot-net`; the app reaches the
nodes by container name (`gg9-iot:10800`, ...). It restarts automatically with Docker. Configure it with environment
variables when calling the script, e.g. `ANTHROPIC_API_KEY=... SIM_SCALE=5 scripts/run-docker.sh`. Do not run it together
with `scripts/run.sh` - both use port 8080 (`HTTP_PORT` changes the host port).

## Architecture

```
 sensors / gateways ──HTTP POST /api/ingest──►  IngestPipeline ──► live cache + trend ring + rate counters
 (embedded simulator                                │
  posts the same way)                               └─► GridGain Data Streamer (3 long-lived streamers)
                                                          ├─ SENSOR_READING_HOT  last 24 sim-hours (memory)
                                                          ├─ SENSOR_READING      all history      (disk)
                                                          └─ SENSOR_LATEST       last value/sensor
 browser ◄── SSE /api/live (1 Hz) ── Monitor        SQL ◄── /api/history  (history popups)
 browser ◄── /api/trends (in-memory 10 s buckets, re-seeded from GridGain on restart)
```

GridGain objects (created on start-up by `GridGainStore.createSchema()` with SQL DDL):

| Object | Purpose |
|---|---|
| zone `IOT_ZONE` (disk, `default` profile) | reference data, latest values, full history; 16 partitions, 3 replicas via `run.sh` (`GG_PARTITIONS`, `GG_REPLICAS`; the app also ALTERs existing zones to match) |
| zone `IOT_MEM_ZONE` (memory, `aimem_hot` profile) | the hot tier; same partitions/replicas |
| `SENSOR` | sensor registry |
| `SENSOR_READING_HOT (SENSOR_ID, TS, VAL, ATTR)` | **last 24 simulated hours, in memory** (20 real min at the default compressed day). Serves the advisor, trend seeding and history popups for ranges within the retention window (15 m by default; longer ranges come from disk) |
| `SENSOR_READING (SENSOR_ID, TS, VAL, ATTR)` | **all history, on disk, never purged.** Serves history popups beyond 24 h (3 d / 7 d), aggregated per minute/hour inside GridGain |
| `SENSOR_LATEST` | latest value per sensor |
| `INGEST_STATS` | per-stream receive rate persisted every 10 s |

Both reading tables are colocated by `SENSOR_ID` with sorted `(SENSOR_ID, TS)` indexes, and each reading is written to
both through its own Data Streamer. **Retention:** `RETENTION_HOURS` (default 24) is measured in *simulated* time while the simulator runs, i.e. scaled by `SIM_DAY_MINUTES`/1440 (real time when `SIM_ENABLED=false`). It applies to the in-memory table only - older
rows are deleted from memory every minute and stay on disk. If memory is lost (all nodes restarted) the app refills the
hot table from disk on start-up. The memory tier needs a storage profile on every node; `start-cluster.sh` configures
`aimem_hot` (`GG_MEM_BYTES`, default 1 GiB per node; the compressed 24 h is only ~30k rows, so memory is no concern; with `SIM_DAY_MINUTES=1440` it is ~2.2 M rows, roughly 0.4-0.5 GiB per node with 3 replicas).

## Efficiency advisor

`Advisor` re-analyses every 30 s using one SQL aggregate over the last `ADVISOR_WINDOW_SECONDS` of `SENSOR_READING`
(default 60 s for the compressed demo day, 900 s otherwise). Rules cover: lights on in empty zones, unoccupied
floors held at comfort temperature, solar gain on east/west facades, over- or under-ventilation from CO₂, free cooling
while the plant runs, surplus lifts in service, out-of-hours baseload, and silent sensors that weaken the analysis.

If `ANTHROPIC_API_KEY` is set (optionally `ANTHROPIC_MODEL`, default `claude-sonnet-5-5`), Claude ranks the findings,
adds a plain-English summary and up to three extra ideas. Only aggregated statistics and the findings are sent, never
raw readings; on any API failure the page falls back to the rules-engine output. API: `GET /api/advice`, `POST /api/advice` (re-analyse).
Suggestions are advisory - the simulated building does not react to them.

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
| `RETENTION_HOURS` | `24` | simulated hours kept in the in-memory tier (real hours if the simulator is off; disk keeps everything) |

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
