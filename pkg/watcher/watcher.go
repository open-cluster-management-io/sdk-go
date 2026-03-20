package watcher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	defaultResyncPeriod = 10 * time.Minute
)

// ConfigMapWatcher watches a ConfigMap and triggers a callback when it changes.
type ConfigMapWatcher struct {
	client        kubernetes.Interface
	namespace     string
	configMapName string
	cancelFunc    context.CancelFunc
	initialHash   string
	onChangeFunc  func()
}

// NewConfigMapWatcher creates a new ConfigMap watcher.
// initData must represent the data the controller is currently running with: the actual
// ConfigMap data if it existed at startup, or the controller's default values if the
// ConfigMap did not exist. Seeding the initial hash this way ensures that any difference
// observed by the informer — a CM that changed between the initial load and the watch
// starting, or a newly created CM whose content differs from the defaults — triggers a
// restart so the controller picks up the new config.
// cancelFunc will be called when the ConfigMap changes, triggering graceful shutdown.
func NewConfigMapWatcher(client kubernetes.Interface, namespace, configMapName string, cancelFunc context.CancelFunc, initData map[string]string) *ConfigMapWatcher {
	return &ConfigMapWatcher{
		client:        client,
		namespace:     namespace,
		configMapName: configMapName,
		cancelFunc:    cancelFunc,
		initialHash:   HashConfigMapData(initData),
	}
}

// SetOnChangeFunc sets a custom function to call when the ConfigMap changes.
// If not set, defaults to calling cancelFunc for graceful shutdown.
func (w *ConfigMapWatcher) SetOnChangeFunc(f func()) {
	w.onChangeFunc = f
}

// Start begins watching the ConfigMap for changes.
// It uses an informer for reliable watching with automatic retries.
func (w *ConfigMapWatcher) Start(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.Info("Starting ConfigMap watcher", "namespace", w.namespace, "configmap", w.configMapName)

	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		w.client,
		defaultResyncPeriod,
		informers.WithNamespace(w.namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", w.configMapName).String()
		}),
	)

	cmInformer := informerFactory.Core().V1().ConfigMaps().Informer()

	_, err := cmInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			cm := obj.(*corev1.ConfigMap)
			if cm.Name != w.configMapName {
				return
			}
			cmHash := hashConfigMap(cm)
			if w.initialHash == "" {
				// No CM existed at startup; treat this as the baseline.
				w.initialHash = cmHash
				logger.V(4).Info("ConfigMap added, initial hash set", "configmap", cm.Name, "hash", cmHash)
			} else if cmHash != w.initialHash {
				// CM existed at startup (initData was provided) but changed in the window
				// between the initial load and the watcher starting. Restart to apply it.
				logger.Info("ConfigMap differs from initial config, restarting",
					"configmap", cm.Name,
					"initialHash", w.initialHash,
					"currentHash", cmHash)
				w.triggerRestart()
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldCM := oldObj.(*corev1.ConfigMap)
			newCM := newObj.(*corev1.ConfigMap)
			if newCM.Name != w.configMapName {
				return
			}

			oldHash := hashConfigMap(oldCM)
			newHash := hashConfigMap(newCM)

			if oldHash != newHash {
				logger.Info("ConfigMap modified, triggering restart",
					"configmap", newCM.Name,
					"oldHash", oldHash,
					"newHash", newHash,
				)
				w.triggerRestart()
			}
		},
		DeleteFunc: func(obj interface{}) {
			cm := obj.(*corev1.ConfigMap)
			if cm.Name != w.configMapName {
				return
			}
			logger.Info("ConfigMap deleted, triggering restart", "configmap", cm.Name)
			w.triggerRestart()
		},
	})
	if err != nil {
		return err
	}

	informerFactory.Start(ctx.Done())

	logger.Info("Waiting for ConfigMap informer cache sync")
	if !cache.WaitForCacheSync(ctx.Done(), cmInformer.HasSynced) {
		return ctx.Err()
	}
	logger.Info("ConfigMap informer cache synced")

	return nil
}

// triggerRestart calls onChangeFunc if set, otherwise cancels the context.
func (w *ConfigMapWatcher) triggerRestart() {
	if w.onChangeFunc != nil {
		w.onChangeFunc()
	} else if w.cancelFunc != nil {
		w.cancelFunc()
	}
}

// HashConfigMapData creates a deterministic string representation of a ConfigMap's data map.
// Keys and values are quoted so that neither can contain the separators used here.
func HashConfigMapData(data map[string]string) string {
	if len(data) == 0 {
		return ""
	}
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%q=%q", k, data[k]))
	}
	return strings.Join(parts, "|")
}

// hashConfigMap returns the hash of a ConfigMap's full data.
func hashConfigMap(cm *corev1.ConfigMap) string {
	if cm == nil {
		return ""
	}
	return HashConfigMapData(cm.Data)
}
