#!/bin/bash

###############################################################################
# Copyright (c) Red Hat, Inc.
# Copyright Contributors to the Open Cluster Management project
###############################################################################

# golangci-lint installation script for sdk-go
# This script can be sourced or executed directly.
#
# Usage:
#   Direct execution:
#     ./ci/lint/install-golangci-lint.sh
#
#   Override version via environment variable:
#     GOLANGCI_LINT_VERSION=v2.5.0 bash ci/lint/install-golangci-lint.sh
#
###############################################################################

set -eo pipefail

# Resolve the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_CONFIG_URL="https://raw.githubusercontent.com/stolostron/sdk-go/main/ci/lint"

# Check if Go is available
if ! command -v go &>/dev/null; then
    echo "Error: Go is not installed or not in PATH"
    exit 1
fi

###############################################################################
# Go version to golangci-lint version compatibility mapping
#
# golangci-lint must be built with Go version >= project's Go version
# Reference: https://github.com/golangci/golangci-lint/issues/5032
#
# Compatibility table (all projects require Go 1.23+):
#   Go 1.24+  -> golangci-lint v2.8.0
#   Go 1.23   -> golangci-lint v2.3.1 (last v2 supporting Go 1.23)
###############################################################################

# Function to get Go version as comparable number (e.g., 1.23 -> 123, 1.25 -> 125)
# Priority: go.mod version (project requirement) > system Go version
get_go_version_num() {
    local go_version=""

    # Priority 1: Read from go.mod (reflects the project's actual Go version requirement)
    if [[ -f "go.mod" ]]; then
        go_version=$(grep -oE '^go [0-9]+\.[0-9]+' go.mod | sed 's/go //')
    fi

    # Priority 2: Fall back to system Go version
    if [[ -z "${go_version}" ]]; then
        go_version=$(go version 2>/dev/null | grep -oE 'go[0-9]+\.[0-9]+' | head -1 | sed 's/go//')
        if [[ -n "${go_version}" ]]; then
            echo "No go.mod found, using system Go version: ${go_version}" >&2
        fi
    fi

    if [[ -n "${go_version}" ]]; then
        # Convert 1.23 to 123, 1.25 to 125, etc.
        echo "${go_version}" | awk -F. '{printf "%d%02d", $1, $2}'
    else
        echo "0"
    fi
}

# Function to select compatible golangci-lint version based on Go version
select_compatible_version() {
    local go_ver_num
    go_ver_num=$(get_go_version_num)

    if [[ "${go_ver_num}" -eq 0 ]]; then
        echo "Warning: Could not detect Go version, using default golangci-lint version" >&2
        echo "${GOLANGCI_LINT_VERSION:-v2.8.0}"
        return
    fi

    # Select compatible version based on Go version
    # All projects require Go 1.23+
    if [[ "${go_ver_num}" -ge 124 ]]; then
        echo "v2.8.0"
    elif [[ "${go_ver_num}" -ge 123 ]]; then
        # v2.3.1 is the last v2 release supporting Go 1.23
        echo "v2.3.1"
    else
        echo "Error: Go version too old. Minimum required: Go 1.23" >&2
        exit 1
    fi
}

# Determine golangci-lint version
# Priority: 1. Environment variable, 2. Auto-detect based on Go version
if [[ -n "${GOLANGCI_LINT_VERSION:-}" ]]; then
    # User explicitly set version via environment variable
    echo "Using user-specified golangci-lint version: ${GOLANGCI_LINT_VERSION}"
else
    # Auto-detect compatible version based on Go version (go.mod priority)
    GOLANGCI_LINT_VERSION=$(select_compatible_version)
    if [[ -f "go.mod" ]]; then
        GO_VERSION=$(grep -oE '^go [0-9]+\.[0-9]+' go.mod | sed 's/go //')
        echo "Detected Go version from go.mod: ${GO_VERSION}"
    else
        GO_VERSION=$(go version 2>/dev/null | grep -oE 'go[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1)
        echo "Detected system Go version: ${GO_VERSION}"
    fi
    echo "Selected compatible golangci-lint version: ${GOLANGCI_LINT_VERSION}"
fi

# Normalize version string (ensure it starts with 'v')
if [[ ! "${GOLANGCI_LINT_VERSION}" =~ ^v ]]; then
    GOLANGCI_LINT_VERSION="v${GOLANGCI_LINT_VERSION}"
fi

# Detect platform and architecture
GOOS="${GOOS:-$(go env GOOS 2>/dev/null || uname -s | tr '[:upper:]' '[:lower:]')}"
GOARCH="${GOARCH:-$(go env GOARCH 2>/dev/null || uname -m)}"

# Normalize architecture names
case "${GOARCH}" in
    x86_64|amd64)
        GOARCH="amd64"
        ;;
    aarch64|arm64)
        GOARCH="arm64"
        ;;
    *)
        echo "Unsupported architecture: ${GOARCH}"
        exit 1
        ;;
esac

# Normalize OS names
case "${GOOS}" in
    Darwin|darwin)
        GOOS="darwin"
        ;;
    Linux|linux)
        GOOS="linux"
        ;;
    *)
        echo "Unsupported OS: ${GOOS}"
        exit 1
        ;;
esac

GOPATH="${GOPATH:-$(go env GOPATH 2>/dev/null || echo "${HOME}/go")}"
INSTALL_DIR="${GOPATH}/bin"

# Function to get installed version
get_installed_version() {
    if command -v golangci-lint &>/dev/null; then
        # Extract version number and normalize to v-prefix format
        # Handles both "v2.5.0" and "2.5.0" formats
        local version
        version=$(golangci-lint --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
        if [[ -n "${version}" ]]; then
            echo "v${version}"
        fi
    fi
}

# Check if the correct version is already installed
INSTALLED_VERSION=$(get_installed_version)
if [[ "${INSTALLED_VERSION}" == "${GOLANGCI_LINT_VERSION}" ]]; then
    echo "golangci-lint ${GOLANGCI_LINT_VERSION} is already installed"
    exit 0
fi

if [[ -n "${INSTALLED_VERSION}" ]]; then
    echo "Upgrading golangci-lint from ${INSTALLED_VERSION} to ${GOLANGCI_LINT_VERSION}"
else
    echo "Installing golangci-lint ${GOLANGCI_LINT_VERSION}"
fi

# Ensure install directory exists
mkdir -p "${INSTALL_DIR}"

# Check if GOPATH/bin is in PATH
if ! echo "${PATH}" | grep -Fq "${INSTALL_DIR}"; then
    echo "Warning: ${INSTALL_DIR} is not in PATH"
    echo "Add it to your PATH: export PATH=\"\${PATH}:${INSTALL_DIR}\""
fi

# Strip the 'v' prefix for the download URL path
VERSION_NUM="${GOLANGCI_LINT_VERSION#v}"

# Construct download URL
DOWNLOAD_URL="https://github.com/golangci/golangci-lint/releases/download/${GOLANGCI_LINT_VERSION}/golangci-lint-${VERSION_NUM}-${GOOS}-${GOARCH}.tar.gz"

echo "Downloading from: ${DOWNLOAD_URL}"

# Download and extract
if ! curl -sfL "${DOWNLOAD_URL}" | tar -C "${INSTALL_DIR}" -zx --strip-components=1 "golangci-lint-${VERSION_NUM}-${GOOS}-${GOARCH}/golangci-lint"; then
    echo "Failed to download or extract golangci-lint"
    exit 1
fi

# Verify installation using the exact path where we installed
get_version_from_path() {
    local binary_path="$1"
    if [[ -x "${binary_path}" ]]; then
        local version
        version=$("${binary_path}" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
        if [[ -n "${version}" ]]; then
            echo "v${version}"
        fi
    fi
}

FINAL_VERSION=$(get_version_from_path "${INSTALL_DIR}/golangci-lint")
if [[ "${FINAL_VERSION}" == "${GOLANGCI_LINT_VERSION}" ]]; then
    echo "Successfully installed golangci-lint ${GOLANGCI_LINT_VERSION} to ${INSTALL_DIR}"
else
    echo "Installation verification failed. Expected ${GOLANGCI_LINT_VERSION}, got ${FINAL_VERSION}"
    exit 1
fi

###############################################################################
# Copy golangci-lint v2 configuration file from the local ci/lint directory
#
# The config file is placed in the project root as .golangci.yml so that
# golangci-lint can find it automatically without -c flag.
###############################################################################

CONFIG_FILE="golangci-v2.yml"
CONFIG_SOURCE="${SCRIPT_DIR}/${CONFIG_FILE}"
LOCAL_CONFIG=".golangci.yml"

# Copy config file if not exists or if user wants to update
copy_config() {
    if [[ -f "${CONFIG_SOURCE}" ]]; then
        echo "Copying golangci-lint v2 config from ${CONFIG_SOURCE}..."
        cp "${CONFIG_SOURCE}" "${LOCAL_CONFIG}"
    else
        echo "Downloading golangci-lint v2 config from remote..."
        if ! curl -sfL "${REMOTE_CONFIG_URL}/${CONFIG_FILE}" -o "${LOCAL_CONFIG}"; then
            echo "Warning: Failed to download config from ${REMOTE_CONFIG_URL}/${CONFIG_FILE}"
            echo "You may need to create ${LOCAL_CONFIG} manually or use -c flag"
            return 1
        fi
    fi
    echo "Configuration saved to ${LOCAL_CONFIG}"
}

# Check if config already exists
if [[ -f "${LOCAL_CONFIG}" ]]; then
    if grep -q "^version: \"2\"" "${LOCAL_CONFIG}" 2>/dev/null; then
        echo "Configuration ${LOCAL_CONFIG} already exists (v2 format)"
    else
        echo "Warning: Existing ${LOCAL_CONFIG} is not v2 format"
        if [[ "${GOLANGCI_UPDATE_CONFIG:-}" == "true" ]]; then
            copy_config
        else
            echo "Set GOLANGCI_UPDATE_CONFIG=true to auto-update the config file"
        fi
    fi
else
    copy_config
fi

echo ""
echo "Setup complete! You can now run: golangci-lint run"
