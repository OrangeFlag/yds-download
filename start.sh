#!/usr/bin/env bash
set -euo pipefail
umask 077
if (( $# < 3 )); then
  echo 'Usage: ./start.sh DESTINATION --source URL [--include NAME | --exclude NAME ...]'
  echo '   or: ./start.sh DESTINATION --catalog /absolute/path/catalog.json'
  exit 2
fi
command -v python3 aria2c cp systemd-run findmnt >/dev/null
python3 -c 'import sys; assert sys.version_info >= (3, 11), "Python 3.11+ required"'
repo_dir=$(cd -- "$(dirname -- "$0")" && pwd)
destination=$(realpath -e -- "$1")
shift
[[ -d "$destination" ]]
mkdir -p -- "$destination/.download"
mapfile -t disk_info < <(python3 - "$destination" <<'PYMETA'
import json, subprocess, sys
row = json.loads(subprocess.check_output(['findmnt', '-J', '-T', sys.argv[1], '-o', 'UUID,TARGET'], text=True))['filesystems'][0]
if not row.get('uuid'):
    raise SystemExit('Destination filesystem has no UUID')
print(row['uuid'])
print(row['target'])
PYMETA
)
[[ ${#disk_info[@]} == 2 ]]
exec systemd-run --user --unit=yds-download --collect \
  --property="WorkingDirectory=$repo_dir" --property=KillSignal=SIGINT --property=SuccessExitStatus=130 \
  --property=TimeoutStopSec=120 --property=StandardOutput=journal --property=StandardError=journal \
  /usr/bin/env python3 "$repo_dir/storage.py" --uuid "${disk_info[0]}" --mount-root "${disk_info[1]}" \
  --log-file "$HOME/.local/state/yds-download/download.log" -- \
  python3 "$repo_dir/download.py" download --destination "$destination" "$@"
