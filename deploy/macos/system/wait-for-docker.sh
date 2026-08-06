#!/bin/zsh
set -euo pipefail

# Wait until Docker engine is available. Works for Docker Desktop and Docker Engine.
PATH="/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin:$PATH"

attempts=0
max_attempts=60
sleep_secs=5

while (( attempts < max_attempts )); do
  if command -v docker >/dev/null 2>&1; then
    if docker info >/dev/null 2>&1; then
      exit 0
    fi
  fi
  attempts=$(( attempts + 1 ))
  sleep $sleep_secs
done

echo "Docker not ready after $((max_attempts * sleep_secs)) seconds" >&2
exit 1
