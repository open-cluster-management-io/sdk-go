package broker

import (
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	grpcauthn "open-cluster-management.io/sdk-go/pkg/cloudevents/server/grpc/authn"
	sar "open-cluster-management.io/sdk-go/pkg/cloudevents/server/grpc/authz/kube"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server/grpc/options"
	services "open-cluster-management.io/sdk-go/test/integration/cloudevents/broker/services"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

func NewGRPCBrokerServer(opt *options.GRPCServerOptions, svc *services.ResourceService) *options.Server {
	return options.NewServer(opt).
		WithService(payload.ManifestBundleEventDataType, svc).
		WithAuthenticator(grpcauthn.NewTokenAuthenticator(util.KubeAuthnClient())).
		WithAuthenticator(grpcauthn.NewMtlsAuthenticator()).
		WithAuthorizer(sar.NewSARAuthorizer(util.KubeAuthzClient()))
}
