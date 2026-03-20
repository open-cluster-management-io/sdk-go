package tls

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// LoadTLSConfigFromConfigMap loads TLS configuration from a ConfigMap in the specified namespace.
// Returns nil if the ConfigMap doesn't exist (not an error — caller should use defaults).
func LoadTLSConfigFromConfigMap(ctx context.Context, client kubernetes.Interface, namespace string) (*TLSConfig, error) {
	cm, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, ConfigMapName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(4).Infof("ConfigMap %s/%s not found, using default TLS config", namespace, ConfigMapName)
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get ConfigMap %s/%s: %w", namespace, ConfigMapName, err)
	}

	return parseTLSConfigFromConfigMap(cm)
}

// parseTLSConfigFromConfigMap parses TLS configuration from a ConfigMap.
func parseTLSConfigFromConfigMap(cm *corev1.ConfigMap) (*TLSConfig, error) {
	if cm == nil {
		return nil, fmt.Errorf("ConfigMap is nil")
	}

	cfg := &TLSConfig{}

	// Parse minimum TLS version
	minVersionStr, ok := cm.Data[ConfigMapKeyMinVersion]
	if !ok || minVersionStr == "" {
		minVersionStr = defaultMinTLSVersion
	}

	minVersion, err := parseTLSVersion(minVersionStr)
	if err != nil {
		return nil, fmt.Errorf("invalid minTLSVersion in ConfigMap: %w", err)
	}
	cfg.MinVersion = minVersion

	// Parse cipher suites
	cipherSuitesStr := cm.Data[ConfigMapKeyCipherSuites]
	if cipherSuitesStr != "" {
		cipherSuites, unsupported := parseCipherSuites(cipherSuitesStr)
		if len(unsupported) > 0 {
			klog.Warningf("Unsupported cipher suites in ConfigMap %s/%s: %v", cm.Namespace, cm.Name, unsupported)
		}
		cfg.CipherSuites = cipherSuites
	}

	return cfg, nil
}
