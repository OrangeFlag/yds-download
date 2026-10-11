#!/usr/bin/env bash
set -euo pipefail
umask 077
if (( $# < 3 )); then
  echo 'Usage: ./start.sh DESTINATION --source URL [--include NAME | --exclude NAME ...]'
  echo '   or: ./start.sh DESTINATION --catalog /absolute/path/catalog.json'
  exit 2
fi
command -v python3 aria2c ffprobe cp systemd-run >/dev/null
python3 -c 'import sys; assert sys.version_info >= (3, 11), "Python 3.11+ required"'
repo_dir=$(cd -- "$(dirname -- "$0")" && pwd)
destination=$(realpath -e -- "$1")
shift
[[ -d "$destination" ]]
mkdir -p -- "$destination/.download"
exec systemd-run --user --unit=yds-download --collect \
  --property="WorkingDirectory=$PWD" --property=KillSignal=SIGINT \
  --property=TimeoutStopSec=90 \
  --property="StandardOutput=append:$destination/.download/download.log" \
  --property="StandardError=append:$destination/.download/download.log" \
  /usr/bin/env python3 "$repo_dir/download.py" download --destination "$destination" "$@"
