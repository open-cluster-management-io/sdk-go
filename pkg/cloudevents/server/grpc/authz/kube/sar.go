package sar

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server/grpc/authz"
	"sync"

	authv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	workv1 "open-cluster-management.io/api/work/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/addon"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/cluster"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/csr"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/event"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/lease"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server/grpc/authn"
)

type SARAuthorizer struct {
	kubeClient kubernetes.Interface
}

// validate SARAuthorizer implement StreamAuthorizer and UnaryAuthorizer
var _ authz.StreamAuthorizer = (*SARAuthorizer)(nil)
var _ authz.UnaryAuthorizer = (*SARAuthorizer)(nil)

// wrappedAuthorizedStream caches the subscription request that is already read.
type wrappedAuthorizedStream struct {
	sync.Mutex

	grpc.ServerStream
	authorizedReq *pbv1.SubscriptionRequest
}

// RecvMsg set the msg from the cache.
func (c *wrappedAuthorizedStream) RecvMsg(m any) error {
	c.Lock()
	defer c.Unlock()

	msg, ok := m.(*pbv1.SubscriptionRequest)
	if !ok {
		return fmt.Errorf("unsupported request type %T", m)
	}

	msg.ClusterName = c.authorizedReq.ClusterName
	msg.Source = c.authorizedReq.Source
	msg.DataType = c.authorizedReq.DataType
	return nil
}

func NewSARAuthorizer(kubeClient kubernetes.Interface) *SARAuthorizer {
	return &SARAuthorizer{
		kubeClient: kubeClient,
	}
}

func (s *SARAuthorizer) AuthorizeRequest(ctx context.Context, req any) error {
	pReq, ok := req.(*pbv1.PublishRequest)
	if !ok {
		return fmt.Errorf("unsupported request type %T", req)
	}

	eventsType, err := types.ParseCloudEventsType(pReq.Event.Type)
	if err != nil {
		return err
	}

	// the event of grpc publish request is the original cloudevent data, we need a `ce-` prefix
	// to get the event attribute
	clusterAttr, ok := pReq.Event.Attributes[fmt.Sprintf("ce-%s", types.ExtensionClusterName)]
	if !ok {
		return fmt.Errorf("missing ce-clustername in event attributes, %v", pReq.Event.Attributes)
	}

	if err := s.authorize(ctx, clusterAttr.GetCeString(), *eventsType); err != nil {
		return err
	}
	return nil
}

func (s *SARAuthorizer) AuthorizeStream(ctx context.Context, ss grpc.ServerStream, info *grpc.StreamServerInfo) (grpc.ServerStream, error) {
	var req pbv1.SubscriptionRequest
	if err := ss.RecvMsg(&req); err != nil {
		return nil, err
	}

	eventDataType, err := types.ParseCloudEventsDataType(req.DataType)
	if err != nil {
		return nil, err
	}

	eventsType := types.CloudEventsType{
		CloudEventsDataType: *eventDataType,
		SubResource:         types.SubResourceSpec,
		Action:              types.WatchRequestAction,
	}

	if err := s.authorize(ss.Context(), req.ClusterName, eventsType); err != nil {
		return nil, err
	}

	return &wrappedAuthorizedStream{ServerStream: ss, authorizedReq: &req}, nil
}

func (s *SARAuthorizer) authorize(ctx context.Context, cluster string, eventsType types.CloudEventsType) error {
	user, groups, err := userInfo(ctx)
	if err != nil {
		return err
	}

	sar, err := toSubjectAccessReview(cluster, user, groups, eventsType)
	if err != nil {
		return err
	}

	created, err := s.kubeClient.AuthorizationV1().SubjectAccessReviews().Create(
		ctx, sar, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	if !created.Status.Allowed {
		return fmt.Errorf("the event %s is not allowed, (cluster=%s, sar=%v, reason=%v)",
			eventsType, cluster, sar.Spec, created.Status)
	}
	return nil
}

func userInfo(ctx context.Context) (user string, groups []string, err error) {
	userValue := ctx.Value(authn.ContextUserKey)
	groupsValue := ctx.Value(authn.ContextGroupsKey)
	if userValue == nil && groupsValue == nil {
		return user, groups, fmt.Errorf("no user and groups in context")
	}

	if userValue != nil {
		var ok bool
		user, ok = userValue.(string)
		if !ok {
			return user, groups, fmt.Errorf("invalid user type in context")
		}
	}

	if groupsValue != nil {
		var ok bool
		groups, ok = groupsValue.([]string)
		if !ok {
			return user, groups, fmt.Errorf("invalid groups in context")
		}
	}

	return user, groups, nil
}

func toSubjectAccessReview(clusterName string, user string, groups []string, eventsType types.CloudEventsType) (*authv1.SubjectAccessReview, error) {
	verb, err := toVerb(eventsType.Action)
	if err != nil {
		return nil, err
	}

	sar := &authv1.SubjectAccessReview{
		Spec: authv1.SubjectAccessReviewSpec{
			ResourceAttributes: &authv1.ResourceAttributes{
				Verb:      verb,
				Namespace: clusterName,
			},
			Groups: groups,
		},
	}

	if len(sar.Spec.Groups) == 0 {
		sar.Spec.User = user
	}

	if eventsType.SubResource == types.SubResourceStatus {
		sar.Spec.ResourceAttributes.Subresource = "status"
	}

	switch eventsType.CloudEventsDataType {
	case cluster.ManagedClusterEventDataType:
		sar.Spec.ResourceAttributes.Group = eventsType.Group
		sar.Spec.ResourceAttributes.Resource = eventsType.Resource
		sar.Spec.ResourceAttributes.Name = clusterName
		return sar, nil
	case addon.ManagedClusterAddOnEventDataType,
		csr.CSREventDataType,
		event.EventEventDataType,
		lease.LeaseEventDataType:
		sar.Spec.ResourceAttributes.Group = eventsType.Group
		sar.Spec.ResourceAttributes.Resource = eventsType.Resource
		return sar, nil
	case payload.ManifestBundleEventDataType:
		sar.Spec.ResourceAttributes.Group = workv1.SchemeGroupVersion.Group
		sar.Spec.ResourceAttributes.Resource = "manifestworks"
		return sar, nil
	default:
		return nil, fmt.Errorf("unsupported event type %s", eventsType.CloudEventsDataType)
	}
}

func toVerb(action types.EventAction) (string, error) {
	switch action {
	case types.CreateRequestAction:
		return "create", nil
	case types.UpdateRequestAction:
		return "update", nil
	case types.DeleteRequestAction:
		return "delete", nil
	case types.WatchRequestAction:
		return "watch", nil
	case types.ResyncRequestAction:
		return "list", nil
	default:
		return "", fmt.Errorf("unsupported action %s", action)
	}
}
