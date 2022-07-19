#!/usr/bin/env bash
set -euo pipefail

go install -race -v \
    storj.io/storj/cmd/satellite@latest \
    storj.io/storj/cmd/storagenode@latest \
    storj.io/storj/cmd/storj-sim@latest \
    storj.io/storj/cmd/versioncontrol@latest \
    storj.io/storj/cmd/uplink@latest \
    storj.io/storj/cmd/identity@latest \
    storj.io/storj/cmd/certificates@latest \
    storj.io/storj/cmd/multinode@latest

go install -race -v storj.io/gateway@latest

echo "=== Finished installing dependencies"

echo "=== Setting up storj-sim..."

until storj-sim -x --host sim network setup; do
    echo "*** redis/postgres is not yet available; waiting for 3s..."
    sleep 3
done

sed -i 's/# metainfo.rate-limiter.enabled: true/metainfo.rate-limiter.enabled: false/g' "$(storj-sim network env SATELLITE_0_DIR)/config.yaml"

echo "=== Disabled rate limiting"

echo "=== Starting storj-sim..."

storj-sim -x network run
