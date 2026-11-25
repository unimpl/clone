#!/usr/bin/env bash
set -euo pipefail

project_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
binary="$project_dir/feishu-im-watch"

if [[ ! -x "$binary" ]]; then
    bash "$project_dir/build.sh"
fi

exec "$binary" "$@"
