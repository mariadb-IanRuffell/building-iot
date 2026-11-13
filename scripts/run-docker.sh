#!/usr/bin/env bash
# Builds the app image and runs it as a container next to the GridGain nodes.
#   APP_CONTAINER (default building-iot-app)   HTTP_PORT (host port, default 8080)
#   GG_NODES (default 3)   GG_CONTAINER (node name prefix, default gg9-iot)   plus any app setting from the README (SIM_*, ANTHROPIC_API_KEY ...)
set -euo pipefail
cd "$(dirname "$0")/.."
APP="${APP_CONTAINER:-building-iot-app}"
PORT="${HTTP_PORT:-8080}"
NAME="${GG_CONTAINER:-gg9-iot}"
NODES="${GG_NODES:-3}"
NET=iot-net

mvn -q -DskipTests package
docker build -q -t building-iot:latest . >/dev/null

# all GridGain containers share a user-defined network so the app can reach them by name
docker network inspect $NET >/dev/null 2>&1 || docker network create $NET >/dev/null
ADDR=""
for i in $(seq 1 "$NODES"); do
  c=$([ "$i" = 1 ] && echo "$NAME" || echo "$NAME-n$i")
  docker network connect $NET "$c" 2>/dev/null || true
  ADDR="$ADDR${ADDR:+,}$c:10800"
done

docker rm -f "$APP" >/dev/null 2>&1 || true
ENVS=(-e "GG_ADDRESSES=$ADDR" -e "GG_REPLICAS=${GG_REPLICAS:-3}")
for v in ANTHROPIC_API_KEY ANTHROPIC_MODEL SIM_ENABLED SIM_MODE SIM_SCALE SIM_DAY_MINUTES RETENTION_HOURS ADVISOR_WINDOW_SECONDS; do
  [ -n "${!v:-}" ] && ENVS+=(-e "$v=${!v}")
done
docker run -d --name "$APP" --network $NET -p "$PORT:8080" --restart unless-stopped "${ENVS[@]}" building-iot:latest >/dev/null
echo "App container '$APP' started: http://localhost:$PORT/"
