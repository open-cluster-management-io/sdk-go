#!/bin/bash

###############################################################################
# Copyright Contributors to the Open Cluster Management project
###############################################################################

# envtest environment setup script for sdk-go
#
# This script installs setup-envtest and downloads the required envtest
# binaries (kube-apiserver, etcd, kubectl). It outputs ONLY the final
# KUBEBUILDER_ASSETS path to stdout; all progress/diagnostic messages
# go to stderr, so Makefiles can capture the path via $(shell ...).
#
# Usage:
#   Direct execution (local):
#     export KUBEBUILDER_ASSETS=$(./ci/envtest/ensure-envtest.sh)
#     go test ./...
#
#   Remote execution (other repos):
#     export KUBEBUILDER_ASSETS=$(curl -fsSL \
#       https://raw.githubusercontent.com/open-cluster-management-io/sdk-go/main/ci/envtest/ensure-envtest.sh | bash)
#     go test ./...
#
#   Makefile integration:
#     test:
#       KUBEBUILDER_ASSETS="$$(curl -fsSL .../ensure-envtest.sh | bash)" go test ./...
#
# Environment variables:
#   ENVTEST_K8S_VERSION    - Override auto-detected K8s version (e.g. "1.34.1")
#   ENVTEST_SETUP_VERSION  - Override auto-detected setup-envtest branch (e.g. "release-0.22")
#   ENVTEST_BIN_DIR        - Directory for envtest binaries (default: /tmp/envtest/bin)
#
###############################################################################

set -eo pipefail

# All informational output goes to stderr
log() {
    echo "$@" >&2
}

###############################################################################
# detect_k8s_version
#
# Determines the K8s version to use for envtest binaries.
# Priority: ENVTEST_K8S_VERSION env var > go.mod k8s.io/api version
###############################################################################
detect_k8s_version() {
    if [[ -n "${ENVTEST_K8S_VERSION:-}" ]]; then
        log "Using user-specified K8s version: ${ENVTEST_K8S_VERSION}"
        echo "${ENVTEST_K8S_VERSION}"
        return
    fi

    if [[ ! -f "go.mod" ]]; then
        log "Error: go.mod not found and ENVTEST_K8S_VERSION not set"
        exit 1
    fi

    # Extract k8s.io/api version: v0.X.Y -> 1.X.Y
    local k8s_mod_version
    k8s_mod_version=$(grep -E '^\s+k8s\.io/api\s+v' go.mod | head -1 | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' || true)

    if [[ -z "${k8s_mod_version}" ]]; then
        log "Error: Could not detect k8s.io/api version from go.mod"
        log "Set ENVTEST_K8S_VERSION explicitly"
        exit 1
    fi

    # v0.X.Y -> 1.X.Y
    local minor patch
    minor=$(echo "${k8s_mod_version}" | cut -d. -f2)
    patch=$(echo "${k8s_mod_version}" | cut -d. -f3)
    local k8s_version="1.${minor}.${patch}"

    log "Detected K8s version from go.mod (k8s.io/api ${k8s_mod_version}): ${k8s_version}"
    echo "${k8s_version}"
}

###############################################################################
# detect_setup_envtest_version
#
# Determines the setup-envtest branch to install.
# Priority: ENVTEST_SETUP_VERSION env var > go.mod controller-runtime version
###############################################################################
detect_setup_envtest_version() {
    if [[ -n "${ENVTEST_SETUP_VERSION:-}" ]]; then
        log "Using user-specified setup-envtest version: ${ENVTEST_SETUP_VERSION}"
        echo "${ENVTEST_SETUP_VERSION}"
        return
    fi

    if [[ ! -f "go.mod" ]]; then
        log "Error: go.mod not found and ENVTEST_SETUP_VERSION not set"
        exit 1
    fi

    # Extract controller-runtime version: v0.X.Y -> release-0.X
    local cr_version
    cr_version=$(grep -E '^\s+sigs\.k8s\.io/controller-runtime\s+v' go.mod | head -1 | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' || true)

    if [[ -z "${cr_version}" ]]; then
        log "Error: Could not detect controller-runtime version from go.mod"
        log "Set ENVTEST_SETUP_VERSION explicitly"
        exit 1
    fi

    # v0.X.Y -> release-0.X
    local major minor
    major=$(echo "${cr_version}" | sed 's/^v//' | cut -d. -f1)
    minor=$(echo "${cr_version}" | cut -d. -f2)
    local setup_version="release-${major}.${minor}"

    log "Detected setup-envtest version from go.mod (controller-runtime ${cr_version}): ${setup_version}"
    echo "${setup_version}"
}

###############################################################################
# install_setup_envtest
#
# Installs setup-envtest into ENVTEST_BIN_DIR using go install.
###############################################################################
install_setup_envtest() {
    local setup_version="$1"
    local bin_dir="$2"

    local setup_envtest="${bin_dir}/setup-envtest"
    local version_marker="${bin_dir}/.setup-envtest-version"

    # Re-install if the binary is missing or was built from a different branch
    if [[ -x "${setup_envtest}" ]] && [[ -f "${version_marker}" ]] && [[ "$(cat "${version_marker}")" == "${setup_version}" ]]; then
        log "setup-envtest@${setup_version} already installed at ${setup_envtest}"
        return
    fi

    log "Installing setup-envtest@${setup_version} into ${bin_dir}..."
    GOBIN="${bin_dir}" go install "sigs.k8s.io/controller-runtime/tools/setup-envtest@${setup_version}"
    echo "${setup_version}" > "${version_marker}"
    log "setup-envtest installed successfully"
}

###############################################################################
# ensure_binaries
#
# Uses setup-envtest to download envtest binaries for the specified K8s
# version. Outputs the assets path to stdout.
###############################################################################
ensure_binaries() {
    local k8s_version="$1"
    local bin_dir="$2"

    local setup_envtest="${bin_dir}/setup-envtest"
    local assets_path=""

    # Extract major.minor for wildcard fallback (e.g. 1.34.2 -> 1.34)
    local k8s_major_minor
    k8s_major_minor=$(echo "${k8s_version}" | cut -d. -f1,2)

    # Try exact version first, then major.minor.x wildcard, then latest
    local try_version
    for try_version in "${k8s_version}" "${k8s_major_minor}.x" "latest"; do
        log "Trying envtest binaries for K8s ${try_version}..."
        assets_path=$("${setup_envtest}" use "${try_version}" --bin-dir "${bin_dir}" -p path 2>/dev/null || true)
        if [[ -n "${assets_path}" ]] && [[ -d "${assets_path}" ]]; then
            log "envtest binaries ready at: ${assets_path} (resolved from ${try_version})"
            echo "${assets_path}"
            return
        fi
        log "Version ${try_version} not available, trying next fallback..."
    done

    log "Error: could not find any usable envtest binaries"
    log "Tried: ${k8s_version}, ${k8s_major_minor}.x, latest"
    exit 1
}

###############################################################################
# main
###############################################################################
main() {
    log "=== envtest environment setup ==="

    # Check prerequisites
    if ! command -v go &>/dev/null; then
        log "Error: Go is not installed or not in PATH"
        exit 1
    fi

    local k8s_version setup_version bin_dir

    k8s_version=$(detect_k8s_version)
    setup_version=$(detect_setup_envtest_version)
    bin_dir="${ENVTEST_BIN_DIR:-/tmp/envtest/bin}"

    mkdir -p "${bin_dir}"

    install_setup_envtest "${setup_version}" "${bin_dir}"
    ensure_binaries "${k8s_version}" "${bin_dir}"
}

main "$@"
