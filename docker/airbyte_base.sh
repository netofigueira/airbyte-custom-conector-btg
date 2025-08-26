#!/usr/bin/env bash
set -euo pipefail

CMD=${AIRBYTE_ENTRYPOINT:-"python /airbyte/integration_code/main.py"}
mkdir -p /source /dest

set +e
$CMD "$@"
EC=$?
echo -n $EC > /source/exitCode.txt || true
echo -n $EC > /dest/exitCode.txt || true
exit $EC

