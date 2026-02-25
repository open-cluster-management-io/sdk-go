# golangci-lint Management for sdk-go

This directory contains scripts and configurations for golangci-lint v2 version management. All builds require **Go 1.23+**.

## Quick Start

Add this to your `Makefile`:

```makefile
.PHONY: lint
lint:
	@bash -o pipefail -c 'curl -fsSL https://raw.githubusercontent.com/open-cluster-management-io/sdk-go/main/ci/lint/run-lint.sh | bash'
```

That's it! The script will:
1. Auto-detect Go version from `go.mod` (falls back to system Go if no `go.mod`)
2. Install a compatible golangci-lint v2 version (if needed)
3. Use the local config file from `ci/lint/golangci-v2.yml` (if no local config exists)
4. Run `golangci-lint run`

**Zero configuration needed** - just run `make lint` and everything works automatically.

**Seamless Go version upgrades**: When you update `go.mod` (e.g., Go 1.23 to 1.25), the script automatically selects the correct golangci-lint v2 version. No manual migration required!

**go.mod-based detection**: The script reads the Go version from your project's `go.mod`, not the system-installed Go. This ensures consistent behavior between local development (where you may have a newer Go) and CI (which matches `go.mod`).

### Configuration Priority

1. **Local config** (`.golangci.yml` or `.golangci.yaml`) - if exists, used as-is
2. **Bundled config** - copied from `ci/lint/golangci-v2.yml`

### Override Version (Optional)

If you need to use a specific version:

```bash
GOLANGCI_LINT_VERSION=v2.8.0 make lint
```

---

## Files Included

| File | Description |
|------|-------------|
| `run-lint.sh` | One-command lint runner (install + config + run) |
| `install-golangci-lint.sh` | Installation script with Go version detection |
| `golangci-v2.yml` | Default config for golangci-lint v2.x |

---

## Go Version Compatibility

The script reads the Go version from `go.mod` and selects the correct golangci-lint v2 version accordingly (falls back to system Go version if no `go.mod` is found).

| Go Version | golangci-lint Version | Config File |
|------------|----------------------|-------------|
| Go 1.24+ | v2.8.0 | `golangci-v2.yml` |
| Go 1.23 | v2.3.1 | `golangci-v2.yml` |

---

## Upgrading Version

To upgrade golangci-lint:

1. Edit the version mapping in both `run-lint.sh` and `install-golangci-lint.sh`
2. Commit and push the changes
3. The new version will be used on the next lint run

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GOLANGCI_CONFIG_DIR` | `/tmp/golangci-lint-config` | Config cache directory |
| `GOLANGCI_UPDATE_CONFIG` | `false` | Force re-copy config if set to `true` |
| `GOLANGCI_LINT_VERSION` | (auto) | Override auto-detected golangci-lint version |

---

## Known Issues and Solutions

### 1. Go Version Mismatch Error

**Error**: `the Go language version used to build golangci-lint is lower than the targeted Go version`

**Solution**: This script automatically selects a compatible version. If you see this error, update the version mapping or use the environment variable override.

### 2. Network Issues

**Problem**: Download fails in CI or behind firewall.

**Solution**: The script skips download if the correct version is already installed. Consider caching the binary or using an internal artifact server.

---

## References

- [golangci-lint Configuration](https://golangci-lint.run/docs/configuration/)
- [Local Installation](https://golangci-lint.run/docs/welcome/install/local/)
- [Issue #5032: Go version compatibility](https://github.com/golangci/golangci-lint/issues/5032)
- [Discussion #3954: Sharing Configs Across Repos](https://github.com/golangci/golangci-lint/discussions/3954)

---

## License

Copyright (c) Red Hat, Inc.
Copyright Contributors to the Open Cluster Management project
