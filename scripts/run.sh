#!/usr/bin/env bash
# Build (if needed) and run the application. Extra env vars: see README.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f target/building-iot.jar ] || mvn -q -DskipTests package
export GG_ADDRESSES="${GG_ADDRESSES:-127.0.0.1:10800,127.0.0.1:10801,127.0.0.1:10802}"
export GG_REPLICAS="${GG_REPLICAS:-3}"
exec java -jar target/building-iot.jar "$@"
