#!/usr/bin/env bash
set -euo pipefail

# Resolve the module directory so this script works from any working directory.
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$script_dir/.."

if command -v go >/dev/null 2>&1; then
    go_binary="$(command -v go)"
elif [[ -x /usr/local/go/bin/go ]]; then
    go_binary=/usr/local/go/bin/go
else
    echo "Go is not installed or is unavailable in PATH." >&2
    exit 1
fi

# Only Kafka is required. The Go tests create and delete their own test topics.
export RUN_KAFKA_INTEGRATION=1
export KAFKA_TEST_BROKERS="${KAFKA_TEST_BROKERS:-localhost:9092}"
exec "$go_binary" test -count=1 -v -timeout=3m ./internal/infra \
    -run '^TestKafkaProducer' "$@"
