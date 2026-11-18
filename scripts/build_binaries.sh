#!/usr/bin/env bash
set -euxo pipefail

OUT=${1}
ADDITIONAL_LDFLAGS=${2:-""}

REVISION=$(git rev-parse --short HEAD)
VERSION=$(git describe --tags --exact-match --match v[0-9]*.[0-9]*.[0-9]*) || VERSION="v0.0.0"

for PLATFORM in linux/arm linux/arm64 linux/amd64 windows/amd64; do
    OS=${PLATFORM%/*}
    ARCH=${PLATFORM#*/}
    EXT=""
    if [ "${OS}" = windows ]; then
        EXT=".exe"
    fi
    CGO_ENABLED=0 GOOS=${OS} GOARCH=${ARCH} GOARM=6 go build \
        -o "${OUT}/gateway_${OS}_${ARCH}${EXT}" \
        -ldflags "-X storj.io/common/version.buildTimestamp=$(date +%s) -X storj.io/common/version.buildCommitHash=${REVISION} -X storj.io/common/version.buildVersion=${VERSION} -X storj.io/common/version.buildRelease=true ${ADDITIONAL_LDFLAGS}"
done
