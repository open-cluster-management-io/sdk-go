package agent

import (
	"context"

	workv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work"
)

func StartWorkAgent(ctx context.Context, clusterName string, config *mqtt.MQTTOptions, codecs ...generic.Codec[*workv1.ManifestWork]) (*work.ClientHolder, error) {
	clientHolder, err := work.NewClientHolderBuilder(config).
		WithClientID(clusterName).
		WithClusterName(clusterName).
		WithCodecs(codecs...).
		NewAgentClientHolder(ctx)
	if err != nil {
		return nil, err
	}

	go clientHolder.ManifestWorkInformer().Informer().Run(ctx.Done())

	return clientHolder, nil
}
