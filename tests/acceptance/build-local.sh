#!/usr/bin/env bash

set -euo pipefail

readonly script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly plugin_root="$(cd -- "${script_dir}/../.." && pwd)"
# RoadRunner v2025.1.15. Use the immutable commit so repeated builds cannot
# silently move if the release tag is ever retargeted.
readonly rr_ref="${ROADRUNNER_REF:-321b817fab1056e404533ca1ddd200e77d1525fd}"
readonly output="${ROADRUNNER_BINARY:-${script_dir}/rr-nexus}"
readonly build_root="$(mktemp -d "${TMPDIR:-/tmp}/rr-nexus-build.XXXXXX")"

cleanup() {
    rm -rf -- "${build_root}"
}
trap cleanup EXIT

git init --quiet "${build_root}/roadrunner"
git -C "${build_root}/roadrunner" remote add origin \
    https://github.com/roadrunner-server/roadrunner.git
git -C "${build_root}/roadrunner" fetch --quiet --depth 1 origin "${rr_ref}"
git -C "${build_root}/roadrunner" checkout --quiet --detach FETCH_HEAD

cd -- "${build_root}/roadrunner"
go mod edit -go=1.26.4
go mod edit -toolchain=go1.26.4
go mod edit \
    "-replace=github.com/temporalio/roadrunner-temporal/v5=${plugin_root}"
go mod tidy

CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-s -w" \
    -o "${output}" \
    ./cmd/rr

"${output}" --version
shasum -a 256 "${output}"
