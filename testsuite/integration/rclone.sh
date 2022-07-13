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

pushd "$TMPDIR" > /dev/null

cleanup() {
    popd > /dev/null
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

rclone config create TestS3 s3 \
    env_auth true \
    provider Minio \
    endpoint "${AWS_ENDPOINT}" \
    chunk_size 64M \
    upload_cutoff 64M

# get rclone version, and trim "-DEV" suffix if it exists, which is added
# if installing rclone using `go install` (as in the storjlabs/ci image)
RCLONE_VERSION=$(head -n1 <(rclone version) | awk '{ print $2 }')
RCLONE_VERSION=${RCLONE_VERSION%-DEV}

# test_all uses some code in the rclone repo, so ensure it's checked out
git clone --branch "$RCLONE_VERSION" --depth 1 https://github.com/rclone/rclone
cd rclone

# only run "fs/sync" for the moment, as the other main test suite
# "fs/operations" has modification time window test failures.
# see https://github.com/storj/gateway-st/issues/46
test_all \
    -backends s3 \
    -remotes TestS3: \
    -tests "fs/sync" \
    -maxtries 1 \
    -verbose \
    -output "$SCRIPTDIR"/../../.build/rclone-integration-tests
