#!/usr/bin/env bash
set -euo pipefail

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

TEST=${1:-}
if [ -n "$TEST" ]; then
	"$SCRIPTDIR"/../testsuite/integration/"$TEST".sh
	exit $?
fi

"$SCRIPTDIR"/../testsuite/integration/awscli.sh
"$SCRIPTDIR"/../testsuite/integration/awscli_multipart.sh
"$SCRIPTDIR"/../testsuite/integration/duplicity.sh
"$SCRIPTDIR"/../testsuite/integration/duplicati.sh
"$SCRIPTDIR"/../testsuite/integration/rclone.sh
"$SCRIPTDIR"/../testsuite/integration/s3fs.sh
