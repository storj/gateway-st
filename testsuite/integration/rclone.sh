#!/usr/bin/env bash
set -Eueo pipefail

log_error() {
    rc=$?
    echo "error code $rc in $(caller) line $LINENO :: ${BASH_COMMAND}"
    exit $rc
}
trap log_error ERR

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# shellcheck source=testsuite/integration/require.sh
source "$SCRIPTDIR"/require.sh

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:=$GATEWAY_0_ACCESS_KEY}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:=$GATEWAY_0_SECRET_KEY}
[ -n "${AWS_ENDPOINT:=http://$GATEWAY_0_ADDR}" ]

# AWS_CA_BUNDLE is configured in require.sh, but for rclone causes problems
# when using a self-signed cert for testing due to the way it uses the AWS SDK
# but also has its own HTTP transport.
# See https://forum.rclone.org/t/mounting-an-amazon-s3-bucket/15106
unset AWS_CA_BUNDLE

# setup tmpdir for testfiles and cleanup
TMPDIR=$(mktemp -d -t tmp.XXXXXXXXXX)
cleanup() {
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

cd "$TMPDIR"; git clone https://github.com/rclone/rclone
RCLONE=$TMPDIR/rclone

pushd "$RCLONE"
    git fetch --tags
    latest_version=$(git tag -l --sort -version:refname | head -1)
    git checkout "$latest_version"

    go build ./fstest/test_all
    go build

    ./rclone config create TestS3 s3 \
        env_auth true \
        provider Minio \
        endpoint "${AWS_ENDPOINT}" \
        chunk_size 64M \
        upload_cutoff 64M

    # only run "fs/sync" for the moment, as the other main test suite
    # "fs/operations" has modification time window test failures.
    # see https://github.com/storj/gateway-st/issues/46
    ./test_all \
        -backends s3 \
        -remotes TestS3: \
        -tests "fs/sync" \
        -maxtries 1 \
        -verbose \
        -output "$SCRIPTDIR"/../../.build/rclone-integration-tests
popd
