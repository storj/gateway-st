#!/usr/bin/env bash
set -euxo pipefail

OUT=${1}
ADDITIONAL_LDFLAGS=${2:-""}

REVISION=$(git rev-parse --short HEAD)
VERSION=$(git describe --tags --exact-match --match v[0-9]*.[0-9]*.[0-9]*) || VERSION="v0.0.0"

for ARCH in arm arm64 amd64; do
    CGO_ENABLED=0 GOOS=linux GOARCH=${ARCH} go build \
        -o "${OUT}/gateway_linux_${ARCH}" \
        -ldflags "-X storj.io/common/version.buildTimestamp=$(date +%s) -X storj.io/common/version.buildCommitHash=${REVISION} -X storj.io/common/version.buildVersion=${VERSION} -X storj.io/common/version.buildRelease=true ${ADDITIONAL_LDFLAGS}"
done
