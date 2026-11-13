#!/usr/bin/env bash
# Starts (or restarts) an N-node GridGain 9.2 cluster in Docker and initialises it.
#   GG_NODES     number of nodes                       (default: 3)
#   GG_LICENSE   path to your GridGain license JSON    (default: ~/gridgain/gridgain-license.json)
#   GG_IMAGE     docker image                          (default: gridgain/gridgain9:9.2.0-b1)
#   GG_CONTAINER container name prefix; node1 uses it as-is, others get -n2, -n3 ... (default: gg9-iot)
# Host ports: node i -> client 10800+(i-1), REST 10300+(i-1).
# Run the app with: GG_ADDRESSES=127.0.0.1:10800,127.0.0.1:10801,127.0.0.1:10802 GG_REPLICAS=3 scripts/run.sh
set -euo pipefail

IMAGE="${GG_IMAGE:-gridgain/gridgain9:9.2.0-b1}"
NAME="${GG_CONTAINER:-gg9-iot}"
NODES="${GG_NODES:-3}"
LICENSE="${GG_LICENSE:-$HOME/gridgain/gridgain-license.json}"
RPORT=10300

[ -f "$LICENSE" ] || { echo "License file not found: $LICENSE (set GG_LICENSE)"; exit 1; }

# The columnar storage engine is not used by this app. Its native library crashes the 9.2.0-b1
# image on Apple-Silicon Docker (SIGSEGV in libcolumnarjni), so the jar is removed before start-up.
PRE='rm -f /opt/gridgain/lib/gridgain-columnar*.jar;'

cname() { [ "$1" = 1 ] && echo "$NAME" || echo "$NAME-n$1"; }

for i in $(seq 1 "$NODES"); do
  c=$(cname "$i")
  if docker ps -a --format '{{.Names}}' | grep -qx "$c"; then
    echo "Container $c exists - starting it"; docker start "$c" >/dev/null; continue
  fi
  if [ "$i" = 1 ]; then
    cfg=''
  else
    N1=$(docker inspect "$NAME" -f '{{.NetworkSettings.IPAddress}}')
    cfg="printf 'ignite { network { port=3344, nodeFinder { netClusterNodes=[\"$N1:3344\"] } } }\n' > /opt/gridgain/etc/gridgain-config.conf;"
  fi
  docker run -d --name "$c" -p "$((10800 + i - 1)):10800" -p "$((10300 + i - 1)):10300" \
    -e JVM_MAX_MEM=2g -e JVM_MIN_MEM=2g --entrypoint sh "$IMAGE" -c \
    "$PRE $cfg exec /usr/local/bin/docker-entrypoint.sh --node-name node$i" >/dev/null
  echo "Started $c"
  [ "$i" = 1 ] && sleep 2
done

echo -n "Waiting for REST endpoint "
for _ in $(seq 1 60); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "localhost:$RPORT/management/v1/cluster/state" || true)
  { [ "$code" = "409" ] || [ "$code" = "200" ]; } && break
  echo -n "."; sleep 2
done
echo

if curl -sf "localhost:$RPORT/management/v1/cluster/state" >/dev/null; then
  echo "Cluster already initialised."
else
  echo -n "Waiting for $NODES nodes to see each other "
  for _ in $(seq 1 60); do
    n=$(curl -s "localhost:$RPORT/management/v1/cluster/topology/physical" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)
    [ "$n" -ge "$NODES" ] && break
    echo -n "."; sleep 2
  done
  echo
  python3 - "$LICENSE" "$RPORT" "$NODES" <<'PY'
import json, sys, urllib.request
lic, port, n = open(sys.argv[1]).read(), sys.argv[2], int(sys.argv[3])
nodes = [f"node{i}" for i in range(1, n + 1)]
# metastorage + cluster-management groups on up to 3 nodes so they survive a node loss
body = json.dumps({"metaStorageNodes": nodes[:3], "cmgNodes": nodes[:3], "clusterName": "iot", "license": lic}).encode()
req = urllib.request.Request(f"http://localhost:{port}/management/v1/cluster/init", body, {"Content-Type": "application/json"})
print("init ->", urllib.request.urlopen(req).status)
PY
  sleep 8
fi
curl -s "localhost:$RPORT/management/v1/cluster/topology/logical" | python3 -c "import json,sys; print('logical topology:', [n['name'] for n in json.load(sys.stdin)])"
