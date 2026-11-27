package tls

import (
	"context"
	"crypto/tls"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestConfigFromFlags(t *testing.T) {
	cases := []struct {
		name             string
		minVersion       string
		cipherSuites     string
		groups           string
		expectNil        bool
		expectError      bool
		expectedMin      uint16
		expectedLen      int
		expectedGroupLen int
	}{
		{
			name:         "all empty returns nil",
			minVersion:   "",
			cipherSuites: "",
			groups:       "",
			expectNil:    true,
			expectError:  false,
		},
		{
			name:        "valid version TLS 1.2",
			minVersion:  "VersionTLS12",
			expectError: false,
			expectedMin: tls.VersionTLS12,
		},
		{
			name:        "valid version TLS 1.3",
			minVersion:  "VersionTLS13",
			expectError: false,
			expectedMin: tls.VersionTLS13,
		},
		{
			name:        "valid version TLSv1.2 format",
			minVersion:  "TLSv1.2",
			expectError: false,
			expectedMin: tls.VersionTLS12,
		},
		{
			name:        "valid version TLS 1.0",
			minVersion:  "VersionTLS10",
			expectError: false,
			expectedMin: tls.VersionTLS10,
		},
		{
			name:        "valid version TLS 1.1",
			minVersion:  "VersionTLS11",
			expectError: false,
			expectedMin: tls.VersionTLS11,
		},
		{
			name:        "invalid version",
			minVersion:  "VersionTLS99",
			expectError: true,
		},
		{
			name:         "valid single cipher",
			minVersion:   "VersionTLS12",
			cipherSuites: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
			expectError:  false,
			expectedMin:  tls.VersionTLS12,
			expectedLen:  1,
		},
		{
			name:         "valid multiple ciphers",
			minVersion:   "VersionTLS12",
			cipherSuites: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
			expectError:  false,
			expectedMin:  tls.VersionTLS12,
			expectedLen:  2,
		},
		{
			name:         "invalid cipher",
			minVersion:   "VersionTLS12",
			cipherSuites: "INVALID-CIPHER",
			expectError:  true,
		},
		{
			name:         "mixed valid and invalid ciphers",
			minVersion:   "VersionTLS12",
			cipherSuites: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,INVALID-CIPHER",
			expectError:  true,
		},
		{
			name:         "only cipher suites without version defaults to TLS 1.2",
			minVersion:   "",
			cipherSuites: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
			expectError:  false,
			expectedMin:  tls.VersionTLS12,
			expectedLen:  1,
		},
		{
			name:         "cipher suites with whitespace",
			minVersion:   "VersionTLS12",
			cipherSuites: " TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 , TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 ",
			expectError:  false,
			expectedMin:  tls.VersionTLS12,
			expectedLen:  2,
		},
		{
			name:             "valid groups with version",
			minVersion:       "VersionTLS12",
			groups:           "x25519,secp256r1",
			expectError:      false,
			expectedMin:      tls.VersionTLS12,
			expectedGroupLen: 2,
		},
		{
			name:             "groups only defaults to TLS 1.2",
			groups:           "X25519MLKEM768",
			expectError:      false,
			expectedMin:      tls.VersionTLS12,
			expectedGroupLen: 1,
		},
		{
			name:             "numeric code points in groups",
			minVersion:       "VersionTLS13",
			groups:           "0x001d,23",
			expectError:      false,
			expectedMin:      tls.VersionTLS13,
			expectedGroupLen: 2,
		},
		{
			name:             "version ciphers and groups together",
			minVersion:       "VersionTLS12",
			cipherSuites:     "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
			groups:           "x25519",
			expectError:      false,
			expectedMin:      tls.VersionTLS12,
			expectedLen:      1,
			expectedGroupLen: 1,
		},
		{
			name:        "whitespace-only groups treated as absent",
			minVersion:  "VersionTLS12",
			groups:      "   ",
			expectError: false,
			expectedMin: tls.VersionTLS12,
		},
		{
			name:             "unsupported group name is skipped with a warning",
			minVersion:       "VersionTLS12",
			groups:           "x25519,FutureGroup",
			expectError:      false,
			expectedMin:      tls.VersionTLS12,
			expectedGroupLen: 1, // only the supported group
		},
		{
			name:        "all group names unsupported errors",
			minVersion:  "VersionTLS12",
			groups:      "FutureGroup1,FutureGroup2",
			expectError: true,
		},
		{
			name:        "separator-only groups errors",
			minVersion:  "VersionTLS12",
			groups:      ",",
			expectError: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ConfigFromFlags(tc.minVersion, tc.cipherSuites, tc.groups)

			if tc.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if tc.expectNil {
				if cfg != nil {
					t.Errorf("expected nil config but got %+v", cfg)
				}
				return
			}

			if cfg == nil {
				t.Fatalf("expected non-nil config")
			}

			if cfg.MinVersion != tc.expectedMin {
				t.Errorf("expected MinVersion %d, got %d", tc.expectedMin, cfg.MinVersion)
			}

			if len(cfg.CipherSuites) != tc.expectedLen {
				t.Errorf("expected %d cipher suites, got %d", tc.expectedLen, len(cfg.CipherSuites))
			}

			if len(cfg.Groups) != tc.expectedGroupLen {
				t.Errorf("expected %d groups, got %d", tc.expectedGroupLen, len(cfg.Groups))
			}
		})
	}
}

// TestConfigFromFlagsRejectsExtraGroupsArgs guards the variadic signature:
// at most one groups argument is accepted.
func TestConfigFromFlagsRejectsExtraGroupsArgs(t *testing.T) {
	_, err := ConfigFromFlags("VersionTLS12", "", "x25519", "secp256r1")
	if err == nil {
		t.Error("expected error for more than one groups argument, got none")
	}
}

func TestVersionToString(t *testing.T) {
	cases := []struct {
		name     string
		version  uint16
		expected string
	}{
		{
			name:     "TLS 1.0",
			version:  tls.VersionTLS10,
			expected: "VersionTLS10",
		},
		{
			name:     "TLS 1.1",
			version:  tls.VersionTLS11,
			expected: "VersionTLS11",
		},
		{
			name:     "TLS 1.2",
			version:  tls.VersionTLS12,
			expected: "VersionTLS12",
		},
		{
			name:     "TLS 1.3",
			version:  tls.VersionTLS13,
			expected: "VersionTLS13",
		},
		{
			name:     "unknown version",
			version:  0x9999,
			expected: "Unknown (0x9999)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := VersionToString(tc.version)
			if result != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, result)
			}
		})
	}
}

func TestCipherSuitesToString(t *testing.T) {
	cases := []struct {
		name     string
		suites   []uint16
		expected string
	}{
		{
			name:     "empty slice",
			suites:   []uint16{},
			expected: "",
		},
		{
			name:     "nil slice",
			suites:   nil,
			expected: "",
		},
		{
			name:     "single cipher",
			suites:   []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256},
			expected: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
		},
		{
			name: "multiple ciphers",
			suites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			},
			expected: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
		},
		{
			name: "unknown cipher ID skipped",
			suites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				0x9999, // unknown cipher
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			},
			expected: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := CipherSuitesToString(tc.suites)
			if result != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, result)
			}
		})
	}
}

func TestConfigToFunc(t *testing.T) {
	cases := []struct {
		name                string
		tlsCfg              *TLSConfig
		expectMinVersion    uint16
		expectMaxVersion    uint16
		expectCipherSuites  bool
		expectedCipherCount int
		expectGroups        bool
		expectedGroupCount  int
	}{
		{
			name: "TLS 1.2 with ciphers",
			tlsCfg: &TLSConfig{
				MinVersion: tls.VersionTLS12,
				CipherSuites: []uint16{
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				},
			},
			expectMinVersion:    tls.VersionTLS12,
			expectCipherSuites:  true,
			expectedCipherCount: 2,
		},
		{
			name: "TLS 1.3 sets MaxVersion",
			tlsCfg: &TLSConfig{
				MinVersion:   tls.VersionTLS13,
				CipherSuites: nil,
			},
			expectMinVersion: tls.VersionTLS13,
			expectMaxVersion: tls.VersionTLS13,
		},
		{
			name: "TLS 1.2 without ciphers",
			tlsCfg: &TLSConfig{
				MinVersion:   tls.VersionTLS12,
				CipherSuites: nil,
			},
			expectMinVersion:   tls.VersionTLS12,
			expectCipherSuites: false,
		},
		{
			name: "TLS 1.3 with ciphers (should not set CipherSuites)",
			tlsCfg: &TLSConfig{
				MinVersion: tls.VersionTLS13,
				CipherSuites: []uint16{
					tls.TLS_AES_128_GCM_SHA256,
				},
			},
			expectMinVersion:   tls.VersionTLS13,
			expectMaxVersion:   tls.VersionTLS13,
			expectCipherSuites: false, // TLS 1.3 branch doesn't set CipherSuites
		},
		{
			name: "groups set CurvePreferences",
			tlsCfg: &TLSConfig{
				MinVersion: tls.VersionTLS12,
				Groups:     []tls.CurveID{tls.X25519, tls.CurveP256},
			},
			expectMinVersion:   tls.VersionTLS12,
			expectCipherSuites: false,
			expectGroups:       true,
			expectedGroupCount: 2,
		},
		{
			name: "empty groups leave CurvePreferences unset",
			tlsCfg: &TLSConfig{
				MinVersion:   tls.VersionTLS12,
				CipherSuites: nil,
				Groups:       nil,
			},
			expectMinVersion:   tls.VersionTLS12,
			expectCipherSuites: false,
			expectGroups:       false,
		},
		{
			name: "ciphers and groups together",
			tlsCfg: &TLSConfig{
				MinVersion:   tls.VersionTLS12,
				CipherSuites: []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256},
				Groups:       []tls.CurveID{tls.X25519},
			},
			expectMinVersion:    tls.VersionTLS12,
			expectCipherSuites:  true,
			expectedCipherCount: 1,
			expectGroups:        true,
			expectedGroupCount:  1,
		},
		{
			name:   "nil config is a no-op",
			tlsCfg: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := &tls.Config{}
			fn := ConfigToFunc(tc.tlsCfg)
			fn(config)

			if tc.tlsCfg == nil {
				return
			}

			if config.MinVersion != tc.expectMinVersion {
				t.Errorf("expected MinVersion %d, got %d", tc.expectMinVersion, config.MinVersion)
			}

			if tc.expectMaxVersion > 0 {
				if config.MaxVersion != tc.expectMaxVersion {
					t.Errorf("expected MaxVersion %d, got %d", tc.expectMaxVersion, config.MaxVersion)
				}
			} else {
				if config.MaxVersion != 0 {
					t.Errorf("expected MaxVersion 0 (unset), got %d", config.MaxVersion)
				}
			}

			if tc.expectCipherSuites {
				if len(config.CipherSuites) != tc.expectedCipherCount {
					t.Errorf("expected %d cipher suites, got %d",
						tc.expectedCipherCount, len(config.CipherSuites))
				}
			} else {
				if len(config.CipherSuites) != 0 {
					t.Errorf("expected no cipher suites, got %d", len(config.CipherSuites))
				}
			}

			if tc.expectGroups {
				if len(config.CurvePreferences) != tc.expectedGroupCount {
					t.Errorf("expected %d curve preferences, got %d",
						tc.expectedGroupCount, len(config.CurvePreferences))
				}
			} else {
				if len(config.CurvePreferences) != 0 {
					t.Errorf("expected no curve preferences, got %d", len(config.CurvePreferences))
				}
			}
		})
	}
}

func TestGetDefaultTLSConfig(t *testing.T) {
	cfg := GetDefaultTLSConfig()

	if cfg == nil {
		t.Fatal("expected non-nil config")
	}

	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("expected MinVersion TLS 1.2, got %d", cfg.MinVersion)
	}

	if cfg.CipherSuites != nil {
		t.Errorf("expected nil CipherSuites, got %v", cfg.CipherSuites)
	}
}

func TestLoadTLSConfigFromConfigMap(t *testing.T) {
	namespace := "test-namespace"

	cases := []struct {
		name             string
		setupClient      func() *fake.Clientset
		expectNil        bool
		expectError      bool
		expectedMin      uint16
		expectedLen      int
		expectedGroupLen int
	}{
		{
			name: "ConfigMap not found returns nil, nil",
			setupClient: func() *fake.Clientset {
				return fake.NewClientset()
			},
			expectNil:   true,
			expectError: false,
		},
		{
			name: "API error returns error",
			setupClient: func() *fake.Clientset {
				client := fake.NewClientset()
				client.PrependReactor("get", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewServiceUnavailable("simulated service unavailable")
				})
				return client
			},
			expectError: true,
		},
		{
			name: "valid ConfigMap with TLS 1.2",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion:   "VersionTLS12",
						ConfigMapKeyCipherSuites: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: false,
			expectedMin: tls.VersionTLS12,
			expectedLen: 1,
		},
		{
			name: "valid ConfigMap with TLS 1.3",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion:   "VersionTLS13",
						ConfigMapKeyCipherSuites: "TLS_AES_128_GCM_SHA256",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: false,
			expectedMin: tls.VersionTLS13,
			expectedLen: 1,
		},
		{
			name: "ConfigMap with invalid version returns error",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion: "InvalidVersion",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: true,
		},
		{
			name: "ConfigMap with unsupported cipher (warning but no error)",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion:   "VersionTLS12",
						ConfigMapKeyCipherSuites: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,UNKNOWN-CIPHER",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: false,
			expectedMin: tls.VersionTLS12,
			expectedLen: 1, // Only the valid cipher
		},
		{
			name: "ConfigMap with all unsupported ciphers returns error",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion:   "VersionTLS12",
						ConfigMapKeyCipherSuites: "UNKNOWN-CIPHER-1,UNKNOWN-CIPHER-2",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: true,
		},
		{
			name: "ConfigMap with empty minVersion defaults to TLS 1.2",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion: "",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: false,
			expectedMin: tls.VersionTLS12,
		},
		{
			name: "ConfigMap with no minVersion key defaults to TLS 1.2",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{},
				}
				return fake.NewClientset(cm)
			},
			expectError: false,
			expectedMin: tls.VersionTLS12,
		},
		{
			name: "ConfigMap with groups parses them",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion: "VersionTLS12",
						ConfigMapKeyGroups:     "x25519,secp256r1",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError:      false,
			expectedMin:      tls.VersionTLS12,
			expectedGroupLen: 2,
		},
		{
			name: "ConfigMap with Go-style group names and mixed case parses them",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion: "VersionTLS12",
						ConfigMapKeyGroups:     "X25519, P384 ,X25519MLKEM768",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError:      false,
			expectedMin:      tls.VersionTLS12,
			expectedGroupLen: 3,
		},
		{
			name: "ConfigMap without groups key leaves Groups empty",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion: "VersionTLS12",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: false,
			expectedMin: tls.VersionTLS12,
		},
		{
			name: "ConfigMap with unsupported group (warning but no error)",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion: "VersionTLS12",
						ConfigMapKeyGroups:     "x25519,FutureGroup",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError:      false,
			expectedMin:      tls.VersionTLS12,
			expectedGroupLen: 1, // Only the valid group
		},
		{
			name: "ConfigMap with all unsupported groups returns error",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion: "VersionTLS12",
						ConfigMapKeyGroups:     "FutureGroup1,FutureGroup2",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: true,
		},
		{
			name: "ConfigMap with separator-only groups returns error",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion: "VersionTLS12",
						ConfigMapKeyGroups:     ",,",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: true,
		},
		{
			name: "ConfigMap with whitespace-only groups is treated as absent",
			setupClient: func() *fake.Clientset {
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						ConfigMapKeyMinVersion: "VersionTLS12",
						ConfigMapKeyGroups:     "   ",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: false,
			expectedMin: tls.VersionTLS12,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := tc.setupClient()
			ctx := context.Background()

			cfg, err := LoadTLSConfigFromConfigMap(ctx, client, namespace)

			if tc.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if tc.expectNil {
				if cfg != nil {
					t.Errorf("expected nil config but got %+v", cfg)
				}
				return
			}

			if cfg == nil {
				t.Fatalf("expected non-nil config")
			}

			if cfg.MinVersion != tc.expectedMin {
				t.Errorf("expected MinVersion %d, got %d", tc.expectedMin, cfg.MinVersion)
			}

			if len(cfg.CipherSuites) != tc.expectedLen {
				t.Errorf("expected %d cipher suites, got %d", tc.expectedLen, len(cfg.CipherSuites))
			}

			if len(cfg.Groups) != tc.expectedGroupLen {
				t.Errorf("expected %d groups, got %d", tc.expectedGroupLen, len(cfg.Groups))
			}
		})
	}
}

func TestStartTLSConfigMapWatcher(t *testing.T) {
	namespace := "test-namespace"

	t.Run("nil onChangeFn returns error", func(t *testing.T) {
		client := fake.NewClientset()
		ctx := context.Background()

		cfg, err := StartTLSConfigMapWatcher(ctx, client, namespace, nil)

		if err == nil {
			t.Errorf("expected error for nil onChangeFn")
		}

		if cfg != nil {
			t.Errorf("expected nil config when error occurred")
		}
	})

	t.Run("ConfigMap not found falls back to defaults", func(t *testing.T) {
		client := fake.NewClientset()
		// Use a generous timeout: w.Start blocks until cache sync before returning.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		changed := make(chan struct{}, 1)
		onChangeFn := func() {
			select {
			case changed <- struct{}{}:
			default:
			}
		}

		cfg, err := StartTLSConfigMapWatcher(ctx, client, namespace, onChangeFn)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if cfg == nil {
			t.Fatal("expected non-nil config")
		}

		if cfg.MinVersion != tls.VersionTLS12 {
			t.Errorf("expected default MinVersion TLS 1.2, got %d", cfg.MinVersion)
		}

		if cfg.CipherSuites != nil {
			t.Errorf("expected nil CipherSuites for defaults, got %v", cfg.CipherSuites)
		}

		select {
		case <-changed:
			t.Errorf("onChangeFn should not be called when ConfigMap doesn't exist")
		case <-time.After(500 * time.Millisecond):
			// expected: no trigger
		}
	})

	t.Run("ConfigMap created with non-defaults triggers restart when no CM at startup", func(t *testing.T) {
		client := fake.NewClientset()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		changed := make(chan struct{}, 1)
		onChangeFn := func() {
			select {
			case changed <- struct{}{}:
			default:
			}
		}

		_, err := StartTLSConfigMapWatcher(ctx, client, namespace, onChangeFn)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Create CM with TLS 1.3 — differs from seeded default TLS 1.2, must trigger.
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: namespace},
			Data:       map[string]string{ConfigMapKeyMinVersion: "VersionTLS13"},
		}
		_, err = client.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create ConfigMap: %v", err)
		}

		select {
		case <-changed:
			// expected
		case <-ctx.Done():
			t.Errorf("onChangeFn should be called when CM created with non-default TLS version")
		}
	})

	t.Run("valid ConfigMap returns parsed config", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ConfigMapName,
				Namespace: namespace,
			},
			Data: map[string]string{
				ConfigMapKeyMinVersion:   "VersionTLS13",
				ConfigMapKeyCipherSuites: "TLS_AES_128_GCM_SHA256",
			},
		}
		client := fake.NewClientset(cm)
		// Use a generous timeout: w.Start blocks until cache sync before returning.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		changed := make(chan struct{}, 1)
		onChangeFn := func() {
			select {
			case changed <- struct{}{}:
			default:
			}
		}

		cfg, err := StartTLSConfigMapWatcher(ctx, client, namespace, onChangeFn)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if cfg == nil {
			t.Fatal("expected non-nil config")
		}

		if cfg.MinVersion != tls.VersionTLS13 {
			t.Errorf("expected MinVersion TLS 1.3, got %d", cfg.MinVersion)
		}

		if len(cfg.CipherSuites) != 1 {
			t.Errorf("expected 1 cipher suite, got %d", len(cfg.CipherSuites))
		}

		select {
		case <-changed:
			t.Errorf("onChangeFn should not be called initially")
		case <-time.After(500 * time.Millisecond):
			// expected: no trigger
		}
	})

	t.Run("watcher calls onChangeFn when ConfigMap is updated", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ConfigMapName,
				Namespace: namespace,
			},
			Data: map[string]string{
				ConfigMapKeyMinVersion: "VersionTLS12",
			},
		}
		client := fake.NewClientset(cm)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		changed := make(chan struct{}, 1)
		onChangeFn := func() {
			select {
			case changed <- struct{}{}:
			default:
			}
		}

		cfg, err := StartTLSConfigMapWatcher(ctx, client, namespace, onChangeFn)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if cfg == nil {
			t.Fatal("expected non-nil config")
		}

		// Update the ConfigMap
		updatedCM := cm.DeepCopy()
		updatedCM.Data[ConfigMapKeyMinVersion] = "VersionTLS13"

		_, err = client.CoreV1().ConfigMaps(namespace).Update(ctx, updatedCM, metav1.UpdateOptions{})
		if err != nil {
			t.Errorf("failed to update ConfigMap: %v", err)
		}

		select {
		case <-changed:
			// expected
		case <-ctx.Done():
			t.Errorf("onChangeFn should be called when ConfigMap is updated")
		}
	})

	t.Run("watcher calls onChangeFn when ConfigMap is deleted", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ConfigMapName,
				Namespace: namespace,
			},
			Data: map[string]string{
				ConfigMapKeyMinVersion: "VersionTLS12",
			},
		}
		client := fake.NewClientset(cm)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		changed := make(chan struct{}, 1)
		onChangeFn := func() {
			select {
			case changed <- struct{}{}:
			default:
			}
		}

		cfg, err := StartTLSConfigMapWatcher(ctx, client, namespace, onChangeFn)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if cfg == nil {
			t.Fatal("expected non-nil config")
		}

		// Delete the ConfigMap
		err = client.CoreV1().ConfigMaps(namespace).Delete(ctx, ConfigMapName, metav1.DeleteOptions{})
		if err != nil {
			t.Errorf("failed to delete ConfigMap: %v", err)
		}

		select {
		case <-changed:
			// expected
		case <-ctx.Done():
			t.Errorf("onChangeFn should be called when ConfigMap is deleted")
		}
	})
}

func TestParseTLSVersion(t *testing.T) {
	cases := []struct {
		name        string
		version     string
		expected    uint16
		expectError bool
	}{
		{
			name:     "VersionTLS10",
			version:  "VersionTLS10",
			expected: tls.VersionTLS10,
		},
		{
			name:     "TLSv1.0",
			version:  "TLSv1.0",
			expected: tls.VersionTLS10,
		},
		{
			name:     "VersionTLS11",
			version:  "VersionTLS11",
			expected: tls.VersionTLS11,
		},
		{
			name:     "TLSv1.1",
			version:  "TLSv1.1",
			expected: tls.VersionTLS11,
		},
		{
			name:     "VersionTLS12",
			version:  "VersionTLS12",
			expected: tls.VersionTLS12,
		},
		{
			name:     "TLSv1.2",
			version:  "TLSv1.2",
			expected: tls.VersionTLS12,
		},
		{
			name:     "VersionTLS13",
			version:  "VersionTLS13",
			expected: tls.VersionTLS13,
		},
		{
			name:     "TLSv1.3",
			version:  "TLSv1.3",
			expected: tls.VersionTLS13,
		},
		{
			name:     "empty defaults to TLS 1.2",
			version:  "",
			expected: tls.VersionTLS12,
		},
		{
			name:     "whitespace defaults to TLS 1.2",
			version:  "   ",
			expected: tls.VersionTLS12,
		},
		{
			name:        "invalid version",
			version:     "VersionTLS99",
			expectError: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseTLSVersion(tc.version)

			if tc.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, result)
			}
		})
	}
}

func TestParseCipherSuites(t *testing.T) {
	cases := []struct {
		name                string
		cipherString        string
		expectedCount       int
		expectedUnsupported int
	}{
		{
			name:          "empty string",
			cipherString:  "",
			expectedCount: 0,
		},
		{
			name:          "whitespace only",
			cipherString:  "   ",
			expectedCount: 0,
		},
		{
			name:          "single valid cipher",
			cipherString:  "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
			expectedCount: 1,
		},
		{
			name:          "multiple valid ciphers",
			cipherString:  "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
			expectedCount: 2,
		},
		{
			name:                "single unsupported cipher",
			cipherString:        "UNKNOWN-CIPHER",
			expectedCount:       0,
			expectedUnsupported: 1,
		},
		{
			name:                "mixed valid and unsupported",
			cipherString:        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,UNKNOWN-CIPHER",
			expectedCount:       1,
			expectedUnsupported: 1,
		},
		{
			name:          "ciphers with whitespace",
			cipherString:  " TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 , TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 ",
			expectedCount: 2,
		},
		{
			name:          "empty entries in list",
			cipherString:  "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
			expectedCount: 2,
		},
		{
			name:          "insecure cipher accepted with warning",
			cipherString:  "TLS_RSA_WITH_AES_128_GCM_SHA256",
			expectedCount: 1,
		},
		{
			name:          "mix of secure and insecure ciphers",
			cipherString:  "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_RSA_WITH_AES_128_GCM_SHA256",
			expectedCount: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			suites, unsupported := ParseCipherSuites(tc.cipherString)

			if len(suites) != tc.expectedCount {
				t.Errorf("expected %d cipher suites, got %d", tc.expectedCount, len(suites))
			}

			if len(unsupported) != tc.expectedUnsupported {
				t.Errorf("expected %d unsupported ciphers, got %d", tc.expectedUnsupported, len(unsupported))
			}
		})
	}
}

func TestParseGroups(t *testing.T) {
	cases := []struct {
		name                string
		groupsString        string
		expectedGroups      []tls.CurveID
		expectedUnsupported int
	}{
		{
			name:         "empty string",
			groupsString: "",
		},
		{
			name:         "whitespace only",
			groupsString: "   ",
		},
		{
			name:           "single IANA name",
			groupsString:   "x25519",
			expectedGroups: []tls.CurveID{tls.X25519},
		},
		{
			name:           "multiple IANA names",
			groupsString:   "x25519,secp256r1,secp384r1",
			expectedGroups: []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384},
		},
		{
			name:           "Go CurveID names",
			groupsString:   "X25519,P256,P384,P521",
			expectedGroups: []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384, tls.CurveP521},
		},
		{
			name:           "case-insensitive matching",
			groupsString:   "X25519,SECP256R1,SecP384r1",
			expectedGroups: []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384},
		},
		{
			name:           "hybrid post-quantum groups",
			groupsString:   "X25519MLKEM768,SecP256r1MLKEM768,secp384r1mlkem1024",
			expectedGroups: []tls.CurveID{tls.X25519MLKEM768, tls.SecP256r1MLKEM768, tls.SecP384r1MLKEM1024},
		},
		{
			name:                "single unsupported group",
			groupsString:        "FutureGroup",
			expectedUnsupported: 1,
		},
		{
			name:                "mixed valid and unsupported",
			groupsString:        "x25519,FutureGroup",
			expectedGroups:      []tls.CurveID{tls.X25519},
			expectedUnsupported: 1,
		},
		{
			name:           "groups with whitespace",
			groupsString:   " x25519 , secp256r1 ",
			expectedGroups: []tls.CurveID{tls.X25519, tls.CurveP256},
		},
		{
			name:           "empty entries in list",
			groupsString:   "x25519,,secp256r1",
			expectedGroups: []tls.CurveID{tls.X25519, tls.CurveP256},
		},
		{
			name:           "hexadecimal code point",
			groupsString:   "0x001d",
			expectedGroups: []tls.CurveID{tls.X25519},
		},
		{
			name:           "decimal code point",
			groupsString:   "29",
			expectedGroups: []tls.CurveID{tls.X25519},
		},
		{
			name:           "uppercase hex prefix",
			groupsString:   "0X001D",
			expectedGroups: []tls.CurveID{tls.X25519},
		},
		{
			name:           "mixed names and code points",
			groupsString:   "x25519,0x0017,P384",
			expectedGroups: []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384},
		},
		{
			name:           "unassigned code point passes through",
			groupsString:   "0x0011",
			expectedGroups: []tls.CurveID{0x0011},
		},
		{
			name:                "zero code point is unsupported",
			groupsString:        "0x0000",
			expectedUnsupported: 1,
		},
		{
			name:                "out-of-range code point is unsupported",
			groupsString:        "0x10000",
			expectedUnsupported: 1,
		},
		{
			name:                "bare hex prefix is unsupported",
			groupsString:        "0x",
			expectedUnsupported: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			groups, unsupported := ParseGroups(tc.groupsString)

			if len(groups) != len(tc.expectedGroups) {
				t.Fatalf("expected %d groups, got %d", len(tc.expectedGroups), len(groups))
			}
			for i, group := range tc.expectedGroups {
				if groups[i] != group {
					t.Errorf("expected group %d to be %d, got %d", i, group, groups[i])
				}
			}

			if len(unsupported) != tc.expectedUnsupported {
				t.Errorf("expected %d unsupported groups, got %d", tc.expectedUnsupported, len(unsupported))
			}
		})
	}
}

// TestParseGroupsAcceptsCurveIDStringNames verifies the advertised contract that
// every name produced by tls.CurveID.String is parseable.
func TestParseGroupsAcceptsCurveIDStringNames(t *testing.T) {
	for id, canonicalName := range curveNamesByID {
		t.Run(canonicalName, func(t *testing.T) {
			groups, unsupported := ParseGroups(id.String())

			if len(unsupported) != 0 {
				t.Errorf("expected no unsupported groups, got %v", unsupported)
			}
			if len(groups) != 1 || groups[0] != id {
				t.Errorf("expected [%d], got %v", id, groups)
			}
		})
	}
}

// TestParseGroupsAcceptsAllRuntimeCurveIDNames enumerates every curve ID the
// linked Go runtime knows about — by brute-forcing the CurveID space and
// skipping IDs whose String() output is the unknown sentinel ("CurveID(<n>)") —
// and asserts each one is fully covered by this package's tables.
//
// Unlike TestParseGroupsAcceptsCurveIDStringNames, which only iterates this
// package's own table and therefore cannot notice a constant the runtime added,
// this test fails when a newer Go toolchain ships a curve constant the tables
// have not learned yet.
//
// When this fails after a Go toolchain upgrade, add the new group to BOTH
// curveIDsByName (its tls.CurveID.String name and its IANA registered name,
// plus any short alias) and curveNamesByID (its IANA registered name as the
// canonical name).
func TestParseGroupsAcceptsAllRuntimeCurveIDNames(t *testing.T) {
	const unknownSentinel = "CurveID("

	found := 0
	for id := 1; id <= 0xFFFF; id++ {
		curve := tls.CurveID(id)
		name := curve.String()
		if strings.HasPrefix(name, unknownSentinel) {
			continue // not a constant this runtime knows
		}
		found++

		t.Run(name, func(t *testing.T) {
			// The Go-style name must parse.
			groups, unsupported := ParseGroups(name)
			if len(unsupported) != 0 {
				t.Errorf("runtime curve name %q is not parseable; add it to curveIDsByName", name)
			}
			if len(groups) != 1 || groups[0] != curve {
				t.Errorf("expected [%d], got %v", curve, groups)
			}

			// The runtime constant must have a canonical IANA name...
			canonical, ok := curveNamesByID[curve]
			if !ok {
				t.Errorf("runtime curve %q (ID %d) has no canonical IANA name in curveNamesByID; "+
					"add its IANA registered name", name, id)
				return
			}

			// ...and that name must parse back to the same curve and round-trip.
			ianaGroups, unsupported := ParseGroups(canonical)
			if len(unsupported) != 0 || len(ianaGroups) != 1 || ianaGroups[0] != curve {
				t.Errorf("canonical IANA name %q for runtime curve %q does not parse to %d "+
					"(groups %v, unsupported %v)", canonical, name, curve, ianaGroups, unsupported)
			}
			if got := GroupsToString([]tls.CurveID{curve}); got != canonical {
				t.Errorf("GroupsToString(%d) = %q, want canonical name %q", curve, got, canonical)
			}
		})
	}

	if found == 0 {
		t.Fatal("no runtime curve IDs enumerated; the CurveID.String unknown sentinel " +
			"format may have changed in this Go version")
	}
}

// TestSupportedGroupNames pins the contract of the exported function: the
// output is sorted, unique, exactly the canonical names from curveNamesByID,
// and every name parses to the curve ID it is canonically named for — i.e.
// curveNamesByID and curveIDsByName agree on every entry.
func TestSupportedGroupNames(t *testing.T) {
	names := SupportedGroupNames()

	// Sorted and unique, as documented: two curve IDs sharing a canonical
	// name would produce a duplicate and break consumers that diff this list
	// against another sorted list of expected names.
	for i := 1; i < len(names); i++ {
		if names[i-1] >= names[i] {
			t.Errorf("names not sorted or contain duplicates at index %d: %q, %q", i, names[i-1], names[i])
		}
	}

	// The output is exactly the canonical names, and each parses to its own
	// ID. The two tables are maintained in parallel; updating one without the
	// other is the maintenance error this catches.
	expected := make(map[string]tls.CurveID, len(curveNamesByID))
	for id, canonical := range curveNamesByID {
		expected[canonical] = id
	}
	if len(names) != len(expected) {
		t.Errorf("expected %d names, got %d", len(expected), len(names))
	}
	for _, name := range names {
		id, ok := expected[name]
		if !ok {
			t.Errorf("unexpected name %q: not a canonical name in curveNamesByID", name)
			continue
		}
		groups, unsupported := ParseGroups(name)
		if len(unsupported) != 0 || len(groups) != 1 || groups[0] != id {
			t.Errorf("canonical name %q (curve ID %d) does not parse to its own ID "+
				"(groups %v, unsupported %v): curveNamesByID and curveIDsByName disagree — "+
				"add the name to curveIDsByName", name, id, groups, unsupported)
		}
	}
}

func TestGroupsToString(t *testing.T) {
	cases := []struct {
		name     string
		groups   []tls.CurveID
		expected string
	}{
		{
			name:     "empty slice",
			groups:   []tls.CurveID{},
			expected: "",
		},
		{
			name:     "nil slice",
			groups:   nil,
			expected: "",
		},
		{
			name:     "single group",
			groups:   []tls.CurveID{tls.X25519},
			expected: "x25519",
		},
		{
			name:     "multiple groups",
			groups:   []tls.CurveID{tls.X25519, tls.CurveP256},
			expected: "x25519,secp256r1",
		},
		{
			name:     "hybrid post-quantum group",
			groups:   []tls.CurveID{tls.X25519MLKEM768},
			expected: "X25519MLKEM768",
		},
		{
			name: "unknown group ID falls back to numeric code point",
			groups: []tls.CurveID{
				tls.X25519,
				0x9999, // unknown group
				tls.CurveP256,
			},
			expected: "x25519,0x9999,secp256r1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := GroupsToString(tc.groups)
			if result != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, result)
			}
		})
	}
}

func TestGroupsRoundTrip(t *testing.T) {
	// Every parseable name must survive a ParseGroups → GroupsToString round trip.
	original := "x25519,secp256r1,secp384r1,secp521r1,X25519MLKEM768,SecP256r1MLKEM768,SecP384r1MLKEM1024"

	groups, unsupported := ParseGroups(original)
	if len(unsupported) != 0 {
		t.Fatalf("expected no unsupported groups, got %v", unsupported)
	}
	if len(groups) != 7 {
		t.Fatalf("expected 7 groups, got %d", len(groups))
	}

	roundTripped := GroupsToString(groups)
	if roundTripped != original {
		t.Errorf("round trip mismatch: expected %s, got %s", original, roundTripped)
	}
}

func TestGroupsRoundTripNumeric(t *testing.T) {
	// Code points must survive a ParseGroups → GroupsToString round trip.
	original := "0x0011,0x0023"

	groups, unsupported := ParseGroups(original)
	if len(unsupported) != 0 {
		t.Fatalf("expected no unsupported groups, got %v", unsupported)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}

	roundTripped := GroupsToString(groups)
	if roundTripped != original {
		t.Errorf("round trip mismatch: expected %s, got %s", original, roundTripped)
	}
}

func TestCipherIDToName(t *testing.T) {
	cases := []struct {
		name     string
		id       uint16
		expected string
	}{
		{
			name:     "known cipher",
			id:       tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			expected: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
		},
		{
			name:     "unknown cipher",
			id:       0x9999,
			expected: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := cipherIDToName(tc.id)
			if result != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, result)
			}
		})
	}
}
