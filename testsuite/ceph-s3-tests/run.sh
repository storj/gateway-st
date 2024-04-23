#!/usr/bin/env bash
set -Eueo pipefail

# use 'make integration-ceph-tests' to run this script

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:=$GATEWAY_0_ACCESS_KEY}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:=$GATEWAY_0_SECRET_KEY}

GATEWAY_HOST=$(echo "${GATEWAY_0_ADDR}" | cut -d : -f 1)
GATEWAY_PORT=$(echo "${GATEWAY_0_ADDR}" | cut -d : -f 2)

BUILD_DIR="$SCRIPTDIR/../../.build"
mkdir -p "$BUILD_DIR"

rm -rf "$BUILD_DIR/s3-tests/"

pushd "$BUILD_DIR"
    # note: tests are pegged at a specific revision to avoid upstream breaking the builds (e.g. config file changes)
    git clone https://github.com/ceph/s3-tests && \
        cd s3-tests && \
        git reset --hard 54c1488a4365afdbe7748eb31809bbb05fa25fb3

    cp "$SCRIPTDIR/storj.conf" ./

    sed -i "s/<access_key>/$AWS_ACCESS_KEY_ID/g" storj.conf
    sed -i "s/<secret_key>/$AWS_SECRET_ACCESS_KEY/g" storj.conf
    sed -i "s/<gateway_host>/$GATEWAY_HOST/g" storj.conf
    sed -i "s/<gateway_port>/$GATEWAY_PORT/g" storj.conf

    # args in quota are not working for some reason so disable check for now
    #shellcheck disable=SC2046
    S3TEST_CONF="storj.conf" tox $( grep "^[^#]" "$SCRIPTDIR/all.tests" | xargs) -- --junitxml="$BUILD_DIR/ceph.xml"
popd
