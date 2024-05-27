package agent

import (
	"context"

	"k8s.io/apimachinery/pkg/util/rand"

	workv1informers "open-cluster-management.io/api/client/work/informers/externalversions/work/v1"
	workv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work"
)

func StartWorkAgent(ctx context.Context,
	clusterName string,
	config *mqtt.MQTTOptions,
	codecs ...generic.Codec[*workv1.ManifestWork],
) (*work.ClientHolder, workv1informers.ManifestWorkInformer, error) {
	clientHolder, informer, err := work.NewClientHolderBuilder(config).
		WithClientID(clusterName + "-" + rand.String(5)).
		WithClusterName(clusterName).
		WithCodecs(codecs...).
		NewAgentClientHolderWithInformer(ctx)
	if err != nil {
		return nil, nil, err
	}

	go informer.Informer().Run(ctx.Done())

	return clientHolder, informer, nil
}
