# envtest Environment Setup for sdk-go

This directory contains a script to set up the [envtest](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/envtest) environment (kube-apiserver, etcd, kubectl binaries) required by integration tests. It replaces the deprecated `storage.googleapis.com/kubebuilder-tools` download method.

## Quick Start

Add this to your `Makefile` as a reusable prerequisite target:

```makefile
ENSURE_ENVTEST_SCRIPT := https://raw.githubusercontent.com/open-cluster-management-io/sdk-go/main/ci/envtest/ensure-envtest.sh

# Reusable target — add as a prerequisite to any target that needs envtest binaries
.PHONY: envtest-setup
envtest-setup:
	$(eval export KUBEBUILDER_ASSETS=$(shell curl -fsSL $(ENSURE_ENVTEST_SCRIPT) | bash))
	@echo "KUBEBUILDER_ASSETS=$(KUBEBUILDER_ASSETS)"

# Example: integration tests depend on envtest-setup
.PHONY: test-integration
test-integration: envtest-setup
	go test ./test/integration/... -coverprofile cover.out

# Example: unit tests that need a real API server
.PHONY: test-unit
test-unit: envtest-setup
	go test ./pkg/... -coverprofile cover.out
```

That's it! The `envtest-setup` target can be used as a prerequisite for any make target that needs envtest binaries. The script will:
1. Auto-detect the K8s version from `go.mod` (`k8s.io/api v0.X.Y` -> `1.X.Y`)
2. Auto-detect the setup-envtest branch from `go.mod` (`controller-runtime v0.X.Y` -> `release-0.X`)
3. Install `setup-envtest` (if needed)
4. Download envtest binaries for the detected K8s version
5. Export `KUBEBUILDER_ASSETS` for downstream targets

**Zero configuration needed** - just add `envtest-setup` as a prerequisite and everything works automatically.

**Seamless version upgrades**: When you update `go.mod` dependencies (e.g., k8s.io/api from v0.31.0 to v0.34.1), the script automatically selects the correct envtest binary version. No manual version bumps required!

---

## How It Works

```
go.mod
  ├── k8s.io/api v0.34.1             → K8s version 1.34.1
  └── controller-runtime v0.22.3     → setup-envtest@release-0.22

ensure-envtest.sh
  1. detect_k8s_version()            → parse go.mod or use $ENVTEST_K8S_VERSION
  2. detect_setup_envtest_version()  → parse go.mod or use $ENVTEST_SETUP_VERSION
  3. install_setup_envtest()         → go install setup-envtest@<branch>
  4. ensure_binaries()               → setup-envtest use <k8s-version> (with fallback)
                                       └── stdout: /path/to/assets
```

All progress messages are printed to **stderr**. Only the final `KUBEBUILDER_ASSETS` path is printed to **stdout**, making it safe to use in `$(shell ...)` or command substitution.

### Version Fallback Strategy

envtest binary releases can lag behind K8s releases. For example, your `go.mod` may reference `k8s.io/api v0.34.2` (K8s 1.34.2), but envtest binaries may only be available up to `1.34.1` or `1.33.0`.

To handle this, the script uses a **three-tier fallback** when resolving envtest binaries:

| Priority | Version pattern | Example | Description |
|----------|----------------|---------|-------------|
| 1 | Exact version | `1.34.2` | Try the precise version detected from `go.mod` |
| 2 | `major.minor.x` wildcard | `1.34.x` | Try the latest available patch for the same minor version |
| 3 | `latest` | latest available | Fall back to the newest available envtest binaries |

```
Example: go.mod has k8s.io/api v0.34.2

  Try 1.34.2       → not available
  Try 1.34.x       → found 1.34.1 ✓  (best match)

Example: go.mod has k8s.io/api v0.99.0

  Try 1.99.0       → not available
  Try 1.99.x       → not available
  Try latest       → found 1.35.0 ✓  (last resort)
```

This ensures the script never fails due to missing envtest binaries, while still preferring the closest matching version.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVTEST_K8S_VERSION` | auto-detected from `go.mod` | Override the K8s version for envtest binaries (e.g. `1.34.1`) |
| `ENVTEST_SETUP_VERSION` | auto-detected from `go.mod` | Override the setup-envtest branch (e.g. `release-0.22`) |
| `ENVTEST_BIN_DIR` | `.bin/envtest` | Directory where setup-envtest and binaries are stored (project-local) |

### Override Examples

```bash
# Use a specific K8s version
ENVTEST_K8S_VERSION=1.31.0 ./ci/envtest/ensure-envtest.sh

# Use a specific setup-envtest version
ENVTEST_SETUP_VERSION=release-0.20 ./ci/envtest/ensure-envtest.sh

# Use a custom binary directory
ENVTEST_BIN_DIR=$HOME/.local/envtest/bin ./ci/envtest/ensure-envtest.sh
```

---

## Version Auto-Detection

The script reads versions from `go.mod` so they stay in sync with your project dependencies:

| go.mod dependency | Detected version | Example |
|-------------------|-----------------|---------|
| `k8s.io/api v0.X.Y` | K8s `1.X.Y` | `v0.34.1` -> `1.34.1` |
| `controller-runtime v0.X.Y` | `release-0.X` | `v0.22.3` -> `release-0.22` |

---

## Troubleshooting

### 1. "go.mod not found" Error

**Cause**: The script is run from a directory without a `go.mod` file.

**Solution**: Either run the script from the project root, or set the version variables explicitly:

```bash
ENVTEST_K8S_VERSION=1.34.1 ENVTEST_SETUP_VERSION=release-0.22 ./ci/envtest/ensure-envtest.sh
```

### 2. "Could not detect k8s.io/api version" Error

**Cause**: Your project does not depend on `k8s.io/api` directly.

**Solution**: Set `ENVTEST_K8S_VERSION` explicitly.

### 3. setup-envtest Download Fails

**Cause**: Network issues or the setup-envtest binary server is unavailable.

**Solution**: The script caches binaries in `ENVTEST_BIN_DIR` (default: `.bin/envtest`). If the binaries already exist, subsequent runs will skip downloading. Consider caching this directory in CI.

> **Note**: Make sure `.bin/` is in your `.gitignore` to avoid committing downloaded binaries.

### 4. "Access denied" from storage.googleapis.com

**Cause**: You are using the old `kubebuilder-tools` download method which has been deprecated.

**Solution**: Switch to this script. It uses `setup-envtest` which downloads from the correct, actively maintained source.

---

## References

- [controller-runtime envtest package](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/envtest)
- [setup-envtest tool](https://pkg.go.dev/sigs.k8s.io/controller-runtime/tools/setup-envtest)
- [kubebuilder-tools deprecation](https://github.com/kubernetes-sigs/controller-runtime/issues/2720)

---

## License

Copyright (c) Red Hat, Inc.
Copyright Contributors to the Open Cluster Management project
