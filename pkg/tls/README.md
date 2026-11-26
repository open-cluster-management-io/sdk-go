# pkg/tls

Helpers for loading, parsing, and applying TLS profile configuration in OCM components.
TLS settings (minimum version, cipher suites, and named groups) are stored in a well-known
ConfigMap (`ocm-tls-profile`) and optionally propagated via command-line flags.

## ConfigMap format

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ocm-tls-profile        # tls.ConfigMapName
  namespace: <component-namespace>
data:
  minTLSVersion: VersionTLS13  # tls.ConfigMapKeyMinVersion
  cipherSuites: TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
  #             ^ tls.ConfigMapKeyCipherSuites
  groups: x25519,secp256r1     # tls.ConfigMapKeyGroups
```

Supported `minTLSVersion` values: `VersionTLS10`, `VersionTLS11`, `VersionTLS12` (default),
`VersionTLS13`.

Cipher suite names use the IANA format (e.g. `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`),
which matches what Go's `crypto/tls` package uses. TLS 1.3 cipher suites are fixed by
the Go runtime and cannot be configured via `cipherSuites`.

Group names use the IANA registered names for the TLS Supported Groups registry
(e.g. `x25519`, `secp256r1`, `X25519MLKEM768`); the names produced by Go's
`tls.CurveID.String` (e.g. `CurveP256`) and the short names `P256`/`P384`/`P521`
are also accepted. Matching is case-insensitive.
Groups restrict the key-exchange groups offered by the server (`tls.Config.CurvePreferences`);
they apply to TLS 1.3 as well as to ECDHE cipher suites in earlier TLS versions. When
`groups` is not set, Go's default group preferences apply. Group names that are not
recognized are logged and skipped.

Components that emit group names can assert that every name they emit is
parseable against `SupportedGroupNames`, turning a silent runtime skip into a
build-time failure.

Groups that are not in the name table can be configured by their numeric code point
from the IANA registry, either hexadecimal with a `0x` prefix (e.g. `0x001d`) or
decimal (e.g. `29`). Code points are passed through as-is, so newly registered
groups can be used without library changes; the Go runtime ignores groups it does
not support at handshake time.

## Use cases

### 1. Component receiving TLS flags

Components that receive `--tls-min-version`, `--tls-cipher-suites`, and `--tls-groups`
flags from their operator do not watch the ConfigMap directly.

`ConfigFromFlags` covers the version, cipher and groups flags; the groups flag is
optional, so callers without a groups flag keep the two-argument form:

```go
import sdktls "open-cluster-management.io/sdk-go/pkg/tls"

// Flags, typically registered via AddFlags on the component's command:
//   --tls-min-version, --tls-cipher-suites, --tls-groups

// Parse TLS configuration from command-line flags.
// Returns nil if no flags were provided (use GetDefaultTLSConfig as fallback).
// Unsupported group names are logged and skipped, mirroring the ConfigMap
// loader: the flag value may have been rendered by a controller built against
// a newer sdk-go whose name table knows groups this build does not. A value
// that yields no groups at all is an error, so a typo can never silently
// fall back to Go's default group preferences.
tlsCfg, err := sdktls.ConfigFromFlags(tlsMinVersionFlag, tlsCipherSuitesFlag, tlsGroupsFlag)
if err != nil {
    return err
}
if tlsCfg == nil {
    tlsCfg = sdktls.GetDefaultTLSConfig()
}

// Apply to a controller-runtime webhook/metrics server via TLSOpts.
mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
    WebhookServer: webhook.NewServer(webhook.Options{
        TLSOpts: []func(*tls.Config){sdktls.ConfigToFunc(tlsCfg)},
    }),
})
```

### 2. Operator or addon agent watching the ConfigMap

Operators and addon agents load the TLS profile at startup and restart when it changes
so that the new settings take effect without a manual pod restart.

`StartTLSConfigMapWatcher` combines the load, watcher setup, and background goroutine
into a single call. It returns the `TLSConfig` active at startup.

```go
import sdktls "open-cluster-management.io/sdk-go/pkg/tls"

// Load current TLS config and start watching for changes.
// Falls back to TLS 1.2 defaults when the ConfigMap does not exist.
tlsCfg, err := sdktls.StartTLSConfigMapWatcher(ctx, kubeClient, namespace, func() {
    klog.Info("TLS ConfigMap changed, restarting")
    os.Exit(0) // Kubernetes will restart the pod with the new settings.
})
if err != nil {
    return err
}

// Use tlsCfg immediately — it reflects what the process is running with.
klog.Infof("TLS min version: %s", sdktls.VersionToString(tlsCfg.MinVersion))
```

### 3. Operator passing TLS settings to managed components via flags

An operator that manages other components can read its own TLS profile and forward the
settings as flags when deploying those components.

```go
tlsCfg, err := sdktls.LoadTLSConfigFromConfigMap(ctx, kubeClient, operatorNamespace)
if err != nil {
    return err
}
if tlsCfg == nil {
    tlsCfg = sdktls.GetDefaultTLSConfig()
}

args := []string{
    "--tls-min-version", sdktls.VersionToString(tlsCfg.MinVersion),
    "--tls-cipher-suites", sdktls.CipherSuitesToString(tlsCfg.CipherSuites),
}
if len(tlsCfg.Groups) > 0 {
    args = append(args, "--tls-groups", sdktls.GroupsToString(tlsCfg.Groups))
}
// Pass args to the managed component's Deployment.
```

## API reference

| Function | Description |
|---|---|
| `StartTLSConfigMapWatcher(ctx, client, namespace, onChangeFn)` | Load config + start watcher in one call. Returns active `*TLSConfig`. |
| `LoadTLSConfigFromConfigMap(ctx, client, namespace)` | Load TLS config from ConfigMap. Returns `nil, nil` if CM not found. |
| `GetDefaultTLSConfig()` | Returns TLS 1.2 with Go's default cipher suites. |
| `ConfigFromFlags(minVersion, cipherSuites, groups...)` | Parse TLS config from flag strings; the groups argument is optional. Returns `nil` if all are empty. Unsupported group names are logged and skipped; a value yielding no groups is an error. |
| `ConfigToFunc(tlsCfg)` | Returns a `func(*tls.Config)` for use with controller-runtime `TLSOpts`. |
| `VersionToString(version)` | Convert a `crypto/tls` version constant to its string name. |
| `CipherSuitesToString(suites)` | Convert cipher suite IDs back to a comma-separated IANA-format string. |
| `ParseGroups(groupsString)` | Convert a comma-separated string of group names or numeric code points to `tls.CurveID` values. Returns unrecognized names. |
| `GroupsToString(groups)` | Convert curve IDs back to a comma-separated string of IANA-format names. |
| `SupportedGroupNames()` | Canonical IANA group names `ParseGroups` recognizes, sorted; for emitters asserting their names are parseable. |

Constants: `ConfigMapName`, `ConfigMapKeyMinVersion`, `ConfigMapKeyCipherSuites`,
`ConfigMapKeyGroups`.
