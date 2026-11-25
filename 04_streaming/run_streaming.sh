#!/bin/bash
# Usage: 04_streaming/run_streaming.sh <part 1-4>
# Starts the Spark streaming app, waits until it is ready, then starts the producer.
PART=${1:?usage: run_streaming.sh <1|2|3|4>}
cd "$(dirname "$0")/.." && source env.sh
rm -rf 04_streaming/stream_input 04_streaming/.app_ready && mkdir -p 04_streaming/stream_input
python 04_streaming/streaming_app.py --part "$PART" --run-seconds 75 &
APP=$!
while [ ! -f 04_streaming/.app_ready ]; do sleep 1; kill -0 $APP 2>/dev/null || exit 1; done
python 04_streaming/producer.py --interval 2
wait $APP
