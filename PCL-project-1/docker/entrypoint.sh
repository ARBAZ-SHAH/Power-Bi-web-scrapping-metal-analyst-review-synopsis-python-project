#!/bin/sh
set -eu

mkdir -p /app/run_logs

exec python /app/analyst_brief_generator_v10.py "$@"
