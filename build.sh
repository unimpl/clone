#!/usr/bin/env bash
set -euo pipefail

project_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd -- "$project_dir"

build_dir=$(mktemp -d "$project_dir/.build.XXXXXXXX")
trap 'rm -f -- "$build_dir/feishu-im-watch"; rmdir -- "$build_dir"' EXIT

go build -o "$build_dir/feishu-im-watch" .
mv -f -- "$build_dir/feishu-im-watch" "$project_dir/feishu-im-watch"
