#!/usr/bin/env bash
set -euo pipefail

log_error() {
	rc=$?
	echo "error code $rc in $(caller) line $LINENO :: ${BASH_COMMAND}"
	exit $rc
}
trap log_error ERR

[ -n "${AWS_ENDPOINT}" ]

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# shellcheck source=testsuite/integration/require.sh
source "$SCRIPTDIR"/require.sh

echo "Testing HTTP/2 is listening"
VERSION=$(curl --silent --output /dev/null --head --http2 --write-out "%{http_version}" "$AWS_ENDPOINT")
require_equal_strings "$VERSION" "2"

echo "Testing HTTP/1.1 is listening"
VERSION=$(curl --silent --output /dev/null --head --http1.1 --write-out "%{http_version}" "$AWS_ENDPOINT")
require_equal_strings "$VERSION" "1.1"