package tls

import (
	"context"
	"crypto/tls"
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
		name         string
		minVersion   string
		cipherSuites string
		expectNil    bool
		expectError  bool
		expectedMin  uint16
		expectedLen  int
	}{
		{
			name:         "both empty returns nil",
			minVersion:   "",
			cipherSuites: "",
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
			cipherSuites: "ECDHE-RSA-AES128-GCM-SHA256",
			expectError:  false,
			expectedMin:  tls.VersionTLS12,
			expectedLen:  1,
		},
		{
			name:         "valid multiple ciphers",
			minVersion:   "VersionTLS12",
			cipherSuites: "ECDHE-RSA-AES128-GCM-SHA256,ECDHE-ECDSA-AES128-GCM-SHA256",
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
			cipherSuites: "ECDHE-RSA-AES128-GCM-SHA256,INVALID-CIPHER",
			expectError:  true,
		},
		{
			name:         "only cipher suites without version defaults to TLS 1.2",
			minVersion:   "",
			cipherSuites: "ECDHE-RSA-AES128-GCM-SHA256",
			expectError:  false,
			expectedMin:  tls.VersionTLS12,
			expectedLen:  1,
		},
		{
			name:         "cipher suites with whitespace",
			minVersion:   "VersionTLS12",
			cipherSuites: " ECDHE-RSA-AES128-GCM-SHA256 , ECDHE-ECDSA-AES128-GCM-SHA256 ",
			expectError:  false,
			expectedMin:  tls.VersionTLS12,
			expectedLen:  2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ConfigFromFlags(tc.minVersion, tc.cipherSuites)

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

			if tc.expectedLen > 0 && len(cfg.CipherSuites) != tc.expectedLen {
				t.Errorf("expected %d cipher suites, got %d", tc.expectedLen, len(cfg.CipherSuites))
			}
		})
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
			expected: "ECDHE-RSA-AES128-GCM-SHA256",
		},
		{
			name: "multiple ciphers",
			suites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			},
			expected: "ECDHE-RSA-AES128-GCM-SHA256,ECDHE-ECDSA-AES128-GCM-SHA256",
		},
		{
			name: "unknown cipher ID skipped",
			suites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				0x9999, // unknown cipher
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			},
			expected: "ECDHE-RSA-AES128-GCM-SHA256,ECDHE-ECDSA-AES128-GCM-SHA256",
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := &tls.Config{}
			fn := ConfigToFunc(tc.tlsCfg)
			fn(config)

			if config.MinVersion != tc.expectMinVersion {
				t.Errorf("expected MinVersion %d, got %d", tc.expectMinVersion, config.MinVersion)
			}

			if tc.expectMaxVersion > 0 && config.MaxVersion != tc.expectMaxVersion {
				t.Errorf("expected MaxVersion %d, got %d", tc.expectMaxVersion, config.MaxVersion)
			}

			if tc.expectCipherSuites {
				if len(config.CipherSuites) != tc.expectedCipherCount {
					t.Errorf("expected %d cipher suites, got %d",
						tc.expectedCipherCount, len(config.CipherSuites))
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
		name        string
		setupClient func() *fake.Clientset
		expectNil   bool
		expectError bool
		expectedMin uint16
		expectedLen int
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
						ConfigMapKeyCipherSuites: "ECDHE-RSA-AES128-GCM-SHA256",
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
						ConfigMapKeyCipherSuites: "ECDHE-RSA-AES128-GCM-SHA256,UNKNOWN-CIPHER",
					},
				}
				return fake.NewClientset(cm)
			},
			expectError: false,
			expectedMin: tls.VersionTLS12,
			expectedLen: 1, // Only the valid cipher
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

			if tc.expectedLen > 0 && len(cfg.CipherSuites) != tc.expectedLen {
				t.Errorf("expected %d cipher suites, got %d", tc.expectedLen, len(cfg.CipherSuites))
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
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		changeCalled := false
		onChangeFn := func() {
			changeCalled = true
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

		// Give the watcher a moment to start
		time.Sleep(50 * time.Millisecond)

		if changeCalled {
			t.Errorf("onChangeFn should not be called when ConfigMap doesn't exist")
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
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		changeCalled := false
		onChangeFn := func() {
			changeCalled = true
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

		// Give the watcher a moment to start
		time.Sleep(50 * time.Millisecond)

		if changeCalled {
			t.Errorf("onChangeFn should not be called initially")
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

		changeCalled := false
		onChangeFn := func() {
			changeCalled = true
			cancel() // Cancel context to stop the watcher
		}

		cfg, err := StartTLSConfigMapWatcher(ctx, client, namespace, onChangeFn)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if cfg == nil {
			t.Fatal("expected non-nil config")
		}

		// Wait for the informer to sync
		time.Sleep(200 * time.Millisecond)

		// Update the ConfigMap
		updatedCM := cm.DeepCopy()
		updatedCM.Data[ConfigMapKeyMinVersion] = "VersionTLS13"

		_, err = client.CoreV1().ConfigMaps(namespace).Update(ctx, updatedCM, metav1.UpdateOptions{})
		if err != nil {
			t.Errorf("failed to update ConfigMap: %v", err)
		}

		// Wait for the event to be processed
		time.Sleep(500 * time.Millisecond)

		if !changeCalled {
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

		changeCalled := false
		onChangeFn := func() {
			changeCalled = true
			cancel() // Cancel context to stop the watcher
		}

		cfg, err := StartTLSConfigMapWatcher(ctx, client, namespace, onChangeFn)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if cfg == nil {
			t.Fatal("expected non-nil config")
		}

		// Wait for the informer to sync
		time.Sleep(200 * time.Millisecond)

		// Delete the ConfigMap
		err = client.CoreV1().ConfigMaps(namespace).Delete(ctx, ConfigMapName, metav1.DeleteOptions{})
		if err != nil {
			t.Errorf("failed to delete ConfigMap: %v", err)
		}

		// Wait for the event to be processed
		time.Sleep(500 * time.Millisecond)

		if !changeCalled {
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
			result, err := parseTLSVersion(tc.version)

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
			cipherString:  "ECDHE-RSA-AES128-GCM-SHA256",
			expectedCount: 1,
		},
		{
			name:          "multiple valid ciphers",
			cipherString:  "ECDHE-RSA-AES128-GCM-SHA256,ECDHE-ECDSA-AES128-GCM-SHA256",
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
			cipherString:        "ECDHE-RSA-AES128-GCM-SHA256,UNKNOWN-CIPHER",
			expectedCount:       1,
			expectedUnsupported: 1,
		},
		{
			name:          "ciphers with whitespace",
			cipherString:  " ECDHE-RSA-AES128-GCM-SHA256 , ECDHE-ECDSA-AES128-GCM-SHA256 ",
			expectedCount: 2,
		},
		{
			name:          "empty entries in list",
			cipherString:  "ECDHE-RSA-AES128-GCM-SHA256,,ECDHE-ECDSA-AES128-GCM-SHA256",
			expectedCount: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			suites, unsupported := parseCipherSuites(tc.cipherString)

			if len(suites) != tc.expectedCount {
				t.Errorf("expected %d cipher suites, got %d", tc.expectedCount, len(suites))
			}

			if len(unsupported) != tc.expectedUnsupported {
				t.Errorf("expected %d unsupported ciphers, got %d", tc.expectedUnsupported, len(unsupported))
			}
		})
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
			expected: "ECDHE-RSA-AES128-GCM-SHA256",
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
