#!/usr/bin/env bash

set -euo pipefail

readonly script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly plugin_root="$(cd -- "${script_dir}/../.." && pwd)"
readonly rr_ref="${ROADRUNNER_REF:-v2025.1.15}"
readonly output="${ROADRUNNER_BINARY:-${script_dir}/rr-nexus}"
readonly build_root="$(mktemp -d "${TMPDIR:-/tmp}/rr-nexus-build.XXXXXX")"

cleanup() {
    rm -rf -- "${build_root}"
}
trap cleanup EXIT

git clone --quiet --depth 1 --branch "${rr_ref}" \
    https://github.com/roadrunner-server/roadrunner.git \
    "${build_root}/roadrunner"

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
