#!/usr/bin/env bash
set -Eueo pipefail

# use 'make integration-ceph-tests' to run this script

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:=$GATEWAY_0_ACCESS_KEY}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:=$GATEWAY_0_SECRET_KEY}

BUILD_DIR="$SCRIPTDIR/../../.build"
mkdir -p "$BUILD_DIR"

rm -rf "$BUILD_DIR/s3-tests/"

pushd "$BUILD_DIR"
    git clone --depth 1 https://github.com/ceph/s3-tests &&  cd s3-tests

    cp "$SCRIPTDIR/storj.conf" ./

    sed -i "s/<access_key>/$AWS_ACCESS_KEY_ID/g" storj.conf
    sed -i "s/<secret_key>/$AWS_SECRET_ACCESS_KEY/g" storj.conf

    # args in quota are not working for some reason so disable check for now
    #shellcheck disable=SC2046
    S3TEST_CONF="storj.conf" tox $( grep "^[^#]" "$SCRIPTDIR/all.tests" | xargs) -- --junitxml="$BUILD_DIR/ceph.xml"
popd