#!/bin/bash
set -e

# ============================================================================
# run-lint.sh - One-command golangci-lint runner for sdk-go
#
# Usage in Makefile:
#   lint:
#     @./ci/lint/run-lint.sh
#
# Configuration priority:
#   1. If local .golangci.yml or .golangci.yaml exists -> use it
#   2. Otherwise -> copy config from ci/lint/golangci-v2.yml to temp dir
#
# Environment variables:
#   GOLANGCI_CONFIG_DIR     - Config cache directory (default: /tmp/golangci-lint-config)
#   GOLANGCI_UPDATE_CONFIG  - Force re-copy config if set to "true"
#   GOLANGCI_LINT_VERSION   - Override auto-detected golangci-lint version
# ============================================================================

# Resolve the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_CACHE_DIR="${GOLANGCI_CONFIG_DIR:-/tmp/golangci-lint-config}"

# Global variables set by detect_versions / install_lint
LINT_VERSION=""
LINT_BIN=""
CONFIG_PATH=""

# Step 1: Detect Go version and determine golangci-lint version
detect_versions() {
    local go_version

    # Priority: go.mod version (project requirement) > system Go version
    if [[ -f "go.mod" ]]; then
        go_version=$(grep -oE '^go [0-9]+\.[0-9]+' go.mod | sed 's/go //')
    fi

    if [[ -z "${go_version:-}" ]]; then
        go_version=$(go version | grep -oE 'go[0-9]+\.[0-9]+' | sed 's/go//')
        echo "No go.mod found, using system Go version: $go_version"
    fi

    local major minor
    major=$(echo "$go_version" | cut -d. -f1)
    minor=$(echo "$go_version" | cut -d. -f2)

    # Determine golangci-lint version based on Go version
    # All projects require Go 1.23+, always use golangci-lint v2
    # v2.4.0+ requires Go 1.24+, v2.3.1 is the last v2 supporting Go 1.23
    if (( major == 1 && minor >= 24 )) || (( major > 1 )); then
        LINT_VERSION="v2.8.0"
    elif (( major == 1 && minor == 23 )); then
        LINT_VERSION="v2.3.1"
    else
        echo "Error: Go ${go_version} is not supported. Minimum required: Go 1.23" >&2
        exit 1
    fi

    # Allow override via environment variable
    if [[ -n "${GOLANGCI_LINT_VERSION:-}" ]]; then
        LINT_VERSION="$GOLANGCI_LINT_VERSION"
        echo "Using override version: $LINT_VERSION"
    fi

    echo "Go version: $go_version -> golangci-lint $LINT_VERSION"
}

# Compare semantic versions: returns 0 if $1 >= $2, 1 otherwise
version_gte() {
    local v1="$1" v2="$2"
    # Remove 'v' prefix
    v1="${v1#v}"
    v2="${v2#v}"

    local IFS='.'
    read -ra v1_parts <<< "$v1"
    read -ra v2_parts <<< "$v2"

    for i in 0 1 2; do
        local p1="${v1_parts[$i]:-0}"
        local p2="${v2_parts[$i]:-0}"
        if (( p1 > p2 )); then
            return 0
        elif (( p1 < p2 )); then
            return 1
        fi
    done
    return 0  # Equal
}

# Check if major version matches (v1.x vs v2.x)
major_version_matches() {
    local current="$1" target="$2"
    local current_major="${current#v}"
    local target_major="${target#v}"
    current_major="${current_major%%.*}"
    target_major="${target_major%%.*}"
    [[ "$current_major" == "$target_major" ]]
}

# Step 2: Install golangci-lint if needed
install_lint() {
    local current_version
    if command -v golangci-lint &>/dev/null; then
        current_version=$(golangci-lint --version 2>/dev/null | grep -oE 'v?[0-9]+\.[0-9]+\.[0-9]+' | head -1)

        # Check if major version matches and current version is >= target
        if major_version_matches "$current_version" "$LINT_VERSION" && version_gte "$current_version" "$LINT_VERSION"; then
            echo "golangci-lint $current_version already installed (>= $LINT_VERSION)"
            LINT_BIN="$(command -v golangci-lint)"
            return 0
        fi
        echo "Current version: $current_version, need: $LINT_VERSION or higher"
    fi

    echo "Installing golangci-lint $LINT_VERSION..."
    local install_dir
    install_dir="$(go env GOPATH)/bin"
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | \
        sh -s -- -b "$install_dir" "$LINT_VERSION"
    LINT_BIN="${install_dir}/golangci-lint"
}

# Step 3: Get config file (local priority, then copy from ci/lint/)
get_config() {
    # Priority 1: Local config
    if [[ -f ".golangci.yml" ]]; then
        echo "Using local config: .golangci.yml"
        CONFIG_PATH=".golangci.yml"
        return 0
    fi

    if [[ -f ".golangci.yaml" ]]; then
        echo "Using local config: .golangci.yaml"
        CONFIG_PATH=".golangci.yaml"
        return 0
    fi

    # Priority 2: Copy from ci/lint/ to temp dir
    mkdir -p "$CONFIG_CACHE_DIR"
    CONFIG_PATH="${CONFIG_CACHE_DIR}/golangci-v2.yml"

    if [[ ! -f "$CONFIG_PATH" ]] || [[ "${GOLANGCI_UPDATE_CONFIG:-false}" == "true" ]]; then
        echo "Copying config: golangci-v2.yml"
        cp "${SCRIPT_DIR}/golangci-v2.yml" "$CONFIG_PATH"
    else
        echo "Using cached config: $CONFIG_PATH"
    fi
}

# Step 4: Run lint
run_lint() {
    echo "Running: $LINT_BIN run -c $CONFIG_PATH ./..."
    echo ""
    "$LINT_BIN" run -c "$CONFIG_PATH" ./...
}

# Main
main() {
    detect_versions
    install_lint
    get_config
    run_lint
}

main "$@"
