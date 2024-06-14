package agent

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/rand"

	workinformers "open-cluster-management.io/api/client/work/informers/externalversions"
	workv1informers "open-cluster-management.io/api/client/work/informers/externalversions/work/v1"
	workv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/store"
)

func StartWorkAgent(ctx context.Context,
	clusterName string,
	config *mqtt.MQTTOptions,
	codecs ...generic.Codec[*workv1.ManifestWork],
) (*work.ClientHolder, workv1informers.ManifestWorkInformer, error) {
	watcherStore := store.NewAgentInformerWatcherStore()

	clientHolder, err := work.NewClientHolderBuilder(config).
		WithClientID(clusterName + "-" + rand.String(5)).
		WithClusterName(clusterName).
		WithCodecs(codecs...).
		WithWorkClientWatcherStore(watcherStore).
		NewAgentClientHolder(ctx)
	if err != nil {
		return nil, nil, err
	}

	factory := workinformers.NewSharedInformerFactoryWithOptions(
		clientHolder.WorkInterface(),
		5*time.Minute,
		workinformers.WithNamespace(clusterName),
	)
	informer := factory.Work().V1().ManifestWorks()
	watcherStore.SetStore(informer.Informer().GetStore())

	go informer.Informer().Run(ctx.Done())

	return clientHolder, informer, nil
}
