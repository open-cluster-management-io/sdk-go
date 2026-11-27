package tls

import (
	"crypto/tls"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"k8s.io/klog/v2"
)

const (
	// ConfigMapName is the well-known name of the ConfigMap containing TLS profile settings.
	ConfigMapName = "ocm-tls-profile"

	// ConfigMapKeyMinVersion is the ConfigMap key for the minimum TLS version.
	ConfigMapKeyMinVersion = "minTLSVersion"

	// ConfigMapKeyCipherSuites is the ConfigMap key for cipher suites
	ConfigMapKeyCipherSuites = "cipherSuites"

	// ConfigMapKeyGroups is the ConfigMap key for TLS named groups (key exchange
	// groups, also known as curve preferences).
	ConfigMapKeyGroups = "groups"
)

// defaultMinTLSVersion is the fallback when no TLS profile is configured
const defaultMinTLSVersion = "VersionTLS12"

// secureCiphersByName maps IANA names → IDs for ciphers in tls.CipherSuites().
var secureCiphersByName map[string]uint16

// insecureCiphersByName maps IANA names → IDs for ciphers in tls.InsecureCipherSuites().
var insecureCiphersByName map[string]uint16

// cipherNamesByID maps cipher suite IDs → IANA names for all known ciphers.
var cipherNamesByID map[uint16]string

// curveIDsByName maps lowercase group names → curve IDs. Both IANA registered
// names (e.g. "secp256r1", "x25519mlkem768") and the names produced by Go's
// tls.CurveID.String (e.g. "CurveP256", "X25519MLKEM768") are accepted; the
// short names "P256"/"P384"/"P521" are kept as additional aliases. Matching is
// case-insensitive.
var curveIDsByName map[string]tls.CurveID

// curveNamesByID maps curve IDs → canonical IANA registered group names.
var curveNamesByID map[tls.CurveID]string

func init() {
	secure := tls.CipherSuites()
	insecure := tls.InsecureCipherSuites()

	secureCiphersByName = make(map[string]uint16, len(secure))
	for _, s := range secure {
		secureCiphersByName[s.Name] = s.ID
	}

	insecureCiphersByName = make(map[string]uint16, len(insecure))
	for _, s := range insecure {
		insecureCiphersByName[s.Name] = s.ID
	}

	cipherNamesByID = make(map[uint16]string, len(secure)+len(insecure))
	for _, s := range secure {
		cipherNamesByID[s.ID] = s.Name
	}
	for _, s := range insecure {
		cipherNamesByID[s.ID] = s.Name
	}

	curveIDsByName = map[string]tls.CurveID{
		"x25519":             tls.X25519,
		"p256":               tls.CurveP256,
		"curvep256":          tls.CurveP256,
		"secp256r1":          tls.CurveP256,
		"p384":               tls.CurveP384,
		"curvep384":          tls.CurveP384,
		"secp384r1":          tls.CurveP384,
		"p521":               tls.CurveP521,
		"curvep521":          tls.CurveP521,
		"secp521r1":          tls.CurveP521,
		"x25519mlkem768":     tls.X25519MLKEM768,
		"secp256r1mlkem768":  tls.SecP256r1MLKEM768,
		"secp384r1mlkem1024": tls.SecP384r1MLKEM1024,
	}

	curveNamesByID = map[tls.CurveID]string{
		tls.X25519:             "x25519",
		tls.CurveP256:          "secp256r1",
		tls.CurveP384:          "secp384r1",
		tls.CurveP521:          "secp521r1",
		tls.X25519MLKEM768:     "X25519MLKEM768",
		tls.SecP256r1MLKEM768:  "SecP256r1MLKEM768",
		tls.SecP384r1MLKEM1024: "SecP384r1MLKEM1024",
	}
}

// TLSConfig represents parsed TLS configuration
type TLSConfig struct {
	MinVersion   uint16
	CipherSuites []uint16
	// Groups restricts the TLS named groups (key exchange groups) the server
	// offers, mapped to tls.Config.CurvePreferences. When empty, Go's default
	// group preferences apply.
	Groups []tls.CurveID
}

// ParseTLSVersion converts a TLS version string to the corresponding crypto/tls constant.
// Accepted formats: "VersionTLS10"/"TLSv1.0" through "VersionTLS13"/"TLSv1.3".
// An empty string defaults to TLS 1.2.
func ParseTLSVersion(version string) (uint16, error) {
	version = strings.TrimSpace(version)
	switch version {
	case "VersionTLS10", "TLSv1.0":
		return tls.VersionTLS10, nil
	case "VersionTLS11", "TLSv1.1":
		return tls.VersionTLS11, nil
	case "VersionTLS12", "TLSv1.2", "":
		// Empty string defaults to TLS 1.2
		return tls.VersionTLS12, nil
	case "VersionTLS13", "TLSv1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("unknown TLS version: %s", version)
	}
}

// ParseCipherSuites converts IANA cipher suite names to Go crypto/tls constants.
// Secure ciphers (tls.CipherSuites) are accepted silently. Insecure ciphers
// (tls.InsecureCipherSuites) are accepted but logged as a warning.
// Returns a list of cipher suite IDs and a list of unrecognized cipher names.
func ParseCipherSuites(cipherString string) ([]uint16, []string) {
	if strings.TrimSpace(cipherString) == "" {
		return nil, nil
	}

	cipherNames := strings.Split(cipherString, ",")
	cipherSuites := make([]uint16, 0, len(cipherNames))
	unsupported := make([]string, 0)

	for _, name := range cipherNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		if id, ok := secureCiphersByName[name]; ok {
			cipherSuites = append(cipherSuites, id)
			continue
		}
		if id, ok := insecureCiphersByName[name]; ok {
			klog.Warningf("Cipher suite %s is insecure and should not be used in production", name)
			cipherSuites = append(cipherSuites, id)
			continue
		}
		unsupported = append(unsupported, name)
	}

	return cipherSuites, unsupported
}

// ParseGroups converts TLS group names to Go crypto/tls curve IDs, for use as
// tls.Config.CurvePreferences. Both IANA registered names (e.g. "secp256r1",
// "X25519MLKEM768") and the names produced by tls.CurveID.String (e.g. "P256")
// are accepted, case-insensitively. Groups that are not in the name table can
// be configured by their numeric code point, either hexadecimal with a "0x"
// prefix (e.g. "0x001d") or decimal (e.g. "29"); code points are passed
// through as-is, so newly registered groups work without library changes.
// Groups that are neither a known name nor a valid code point are skipped and
// returned so callers can report them.
// Returns a list of curve IDs and a list of unrecognized group names.
func ParseGroups(groupsString string) ([]tls.CurveID, []string) {
	if strings.TrimSpace(groupsString) == "" {
		return nil, nil
	}

	groupNames := strings.Split(groupsString, ",")
	groups := make([]tls.CurveID, 0, len(groupNames))
	unsupported := make([]string, 0)

	for _, name := range groupNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		if id, ok := curveIDsByName[strings.ToLower(name)]; ok {
			groups = append(groups, id)
			continue
		}

		if id, ok := parseCurveID(name); ok {
			groups = append(groups, id)
			continue
		}

		unsupported = append(unsupported, name)
	}

	return groups, unsupported
}

// parseCurveID parses a raw TLS group code point, either hexadecimal with a
// "0x"/"0X" prefix or decimal. It rejects empty digits, values that do not fit
// a curve ID, and the reserved zero value.
func parseCurveID(name string) (tls.CurveID, bool) {
	base := 10
	digits := name
	if strings.HasPrefix(strings.ToLower(name), "0x") {
		base = 16
		digits = name[2:]
	}

	if digits == "" {
		return 0, false
	}

	id, err := strconv.ParseUint(digits, base, 16)
	if err != nil || id == 0 {
		return 0, false
	}

	return tls.CurveID(id), true
}

// GetDefaultTLSConfig returns a TLS config with safe defaults (TLS 1.2)
func GetDefaultTLSConfig() *TLSConfig {
	return &TLSConfig{
		MinVersion:   tls.VersionTLS12,
		CipherSuites: nil, // Use Go's default cipher suites for TLS 1.2
	}
}

// ConfigFromFlags creates TLS config from command-line flags.
// Accepted formats: minVersion ("VersionTLS12"), cipherSuites (comma-joined
// IANA names), and optionally groups (comma-joined IANA names or numeric code
// points, see ParseGroups). The groups parameter is optional for backward
// compatibility: ConfigFromFlags(minVersion, cipherSuites) keeps working, and
// components that take a --tls-groups flag pass it as a third argument:
//
//	ConfigFromFlags(tlsMinVersion, tlsCipherSuites, tlsGroups)
//
// Unsupported group names are logged and skipped, mirroring the ConfigMap
// loader: the group name table is a frozen snapshot of the IANA registry, and
// a component can receive a flag value rendered by a controller built against
// a newer sdk-go, so an unknown name must not take the component down.
// Dropping groups only ever narrows the offered set, never widens it; a value
// that yields no groups at all is still an error, so the flag can never
// silently fall back to Go's default group preferences.
func ConfigFromFlags(minVersion, cipherSuites string, groups ...string) (*TLSConfig, error) {
	minVersion = strings.TrimSpace(minVersion)
	cipherSuites = strings.TrimSpace(cipherSuites)

	if len(groups) > 1 {
		return nil, fmt.Errorf("ConfigFromFlags takes at most one groups argument, got %d", len(groups))
	}
	groupsFlag := ""
	if len(groups) == 1 {
		groupsFlag = strings.TrimSpace(groups[0])
	}

	if minVersion == "" && cipherSuites == "" && groupsFlag == "" {
		return nil, nil // No flags provided
	}

	cfg := &TLSConfig{}

	// Parse min version
	if minVersion != "" {
		ver, err := ParseTLSVersion(minVersion)
		if err != nil {
			return nil, fmt.Errorf("invalid --tls-min-version: %w", err)
		}
		cfg.MinVersion = ver
	} else {
		cfg.MinVersion = tls.VersionTLS12
	}

	// Parse cipher suites
	if cipherSuites != "" {
		suites, unsupported := ParseCipherSuites(cipherSuites)
		if len(unsupported) > 0 {
			return nil, fmt.Errorf("unsupported cipher suites: %v", unsupported)
		}
		cfg.CipherSuites = suites
	}

	// Parse groups (curve preferences). Unsupported names are skipped with a
	// warning rather than an error: the value may have been rendered by a
	// controller built against a newer sdk-go whose name table knows groups
	// this build does not. Dropping only narrows the offered set; a value
	// that yields no groups at all is an error so the flag can never
	// silently fall back to Go's default group preferences.
	if groupsFlag != "" {
		parsed, unsupported := ParseGroups(groupsFlag)
		if len(unsupported) > 0 {
			klog.Warningf("Skipping unsupported TLS groups in --tls-groups: %v", unsupported)
		}
		if len(parsed) == 0 {
			return nil, fmt.Errorf("invalid --tls-groups: no supported groups found")
		}
		cfg.Groups = parsed
	}

	return cfg, nil
}

// VersionToString converts a TLS version constant to its string representation
func VersionToString(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "VersionTLS10"
	case tls.VersionTLS11:
		return "VersionTLS11"
	case tls.VersionTLS12:
		return "VersionTLS12"
	case tls.VersionTLS13:
		return "VersionTLS13"
	default:
		return fmt.Sprintf("Unknown (0x%04x)", version)
	}
}

// CipherSuitesToString converts cipher suite IDs back to IANA names
func CipherSuitesToString(suites []uint16) string {
	if len(suites) == 0 {
		return ""
	}

	names := make([]string, 0, len(suites))
	for _, suite := range suites {
		name := cipherIDToName(suite)
		if name != "" {
			names = append(names, name)
		}
	}
	return strings.Join(names, ",")
}

// GroupsToString converts curve IDs back to a comma-separated string of IANA
// registered group names. IDs without a known name (e.g. a group configured by
// numeric code point) are emitted as their hexadecimal code point so they
// round-trip through ParseGroups.
func GroupsToString(groups []tls.CurveID) string {
	if len(groups) == 0 {
		return ""
	}

	names := make([]string, 0, len(groups))
	for _, group := range groups {
		name := curveNamesByID[group]
		if name == "" {
			name = fmt.Sprintf("0x%04x", uint16(group))
		}
		names = append(names, name)
	}
	return strings.Join(names, ",")
}

// SupportedGroupNames returns the canonical IANA registered group names that
// ParseGroups recognizes, sorted for determinism. Aliases (the names produced
// by tls.CurveID.String and the short P256-style names) are not included.
func SupportedGroupNames() []string {
	names := make([]string, 0, len(curveNamesByID))
	for _, name := range curveNamesByID {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ConfigToFunc returns a function that applies the TLS configuration to a tls.Config.
// It is suitable for use with controller-runtime's TLSOpts (webhook/metrics servers).
// If tlsCfg is nil (e.g. returned by ConfigFromFlags when no flags are set), the
// returned function is a no-op that leaves the tls.Config unchanged.
func ConfigToFunc(tlsCfg *TLSConfig) func(*tls.Config) {
	if tlsCfg == nil {
		return func(*tls.Config) {}
	}
	return func(config *tls.Config) {
		config.MinVersion = tlsCfg.MinVersion
		if tlsCfg.MinVersion == tls.VersionTLS13 {
			config.MaxVersion = tls.VersionTLS13
		} else if len(tlsCfg.CipherSuites) > 0 {
			config.CipherSuites = tlsCfg.CipherSuites
		}
		if len(tlsCfg.Groups) > 0 {
			config.CurvePreferences = tlsCfg.Groups
		}
	}
}

// cipherIDToName converts a cipher suite ID to its IANA name.
func cipherIDToName(id uint16) string {
	return cipherNamesByID[id]
}
