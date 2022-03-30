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

#setup tmpdir for testfiles and cleanup
TMPDIR=$(mktemp -d -t tmp.XXXXXXXXXX)
cleanup(){
	rm -rf "$TMPDIR"
}
trap cleanup EXIT

SRC_DIR=$TMPDIR/source
DST_DIR=$TMPDIR/dst
mkdir -p "$SRC_DIR" "$DST_DIR"

random_bytes_file () {
	size=$1
	output=$2
	dd if=/dev/urandom of="$output" count=1 bs="$size" >/dev/null 2>&1
}

random_bytes_file "1MiB"  "$SRC_DIR/backup-testfile-1MiB"  # create 1MiB file of random bytes (remote)
random_bytes_file "10MiB" "$SRC_DIR/backup-testfile-10MiB" # create 10MiB file of random bytes (remote)

BUCKET="backup-bucket"

# trim the protocol from the start of the endpoint (if present)
SERVER_NAME="${AWS_ENDPOINT#*//}"
USE_SSL="true"
if [[ ${AWS_CA_BUNDLE-x} == x ]]; then
	USE_SSL="false"
fi

duplicati-cli backup "s3://$BUCKET" "$SRC_DIR" --s3-server-name="$SERVER_NAME" \
	--passphrase=my-pass \
	--use-ssl=$USE_SSL \
	--debug-output=true \
	--auth-username="$AWS_ACCESS_KEY_ID" \
	--auth-password="$AWS_SECRET_ACCESS_KEY"

duplicati-cli restore "s3://$BUCKET" --restore-path="$DST_DIR" \
	--s3-server-name="$SERVER_NAME" \
	--passphrase=my-pass \
	--use-ssl=$USE_SSL \
	--debug-output=true \
	--auth-username="$AWS_ACCESS_KEY_ID" \
	--auth-password="$AWS_SECRET_ACCESS_KEY"

require_equal_files_content "$SRC_DIR/backup-testfile-1MiB"  "$DST_DIR/backup-testfile-1MiB"
require_equal_files_content "$SRC_DIR/backup-testfile-10MiB" "$DST_DIR/backup-testfile-10MiB"
