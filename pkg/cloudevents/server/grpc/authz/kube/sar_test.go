package sar

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/grpc"
	authv1 "k8s.io/api/authorization/v1"
	certificatesv1 "k8s.io/api/certificates/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workv1 "open-cluster-management.io/api/work/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/cluster"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/csr"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/lease"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/serviceaccount"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	genericpayload "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/payload"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/server/grpc/authn"
	"open-cluster-management.io/sdk-go/pkg/server/grpc/authz"
)

func newPublishRequest(t *testing.T, eventsType types.CloudEventsType, clusterName string, data any) *pbv1.PublishRequest {
	t.Helper()

	evt := &pbv1.CloudEvent{
		SpecVersion: "1.0",
		Id:          "test-id",
		Source:      "test-source",
		Type:        eventsType.String(),
		Attributes:  map[string]*pbv1.CloudEventAttributeValue{},
	}
	if clusterName != "" {
		evt.Attributes["ce-clustername"] = &pbv1.CloudEventAttributeValue{
			Attr: &pbv1.CloudEventAttributeValue_CeString{CeString: clusterName},
		}
	}
	if data != nil {
		raw, err := json.Marshal(data)
		if err != nil {
			t.Fatalf("failed to marshal event data: %v", err)
		}
		evt.Data = &pbv1.CloudEvent_BinaryData{BinaryData: raw}
	}
	return &pbv1.PublishRequest{Event: evt}
}

func userContext(user string) func() context.Context {
	return func() context.Context {
		return context.WithValue(context.Background(), authn.ContextUserKey, user)
	}
}

func TestSARAuthorizeRequest(t *testing.T) {
	type testCase struct {
		name    string
		request *pbv1.PublishRequest
		userCtx func() context.Context
		// allow decides the SubjectAccessReview result; when nil, the request must be rejected before any SAR is made.
		allow        func(sar *authv1.SubjectAccessReview) bool
		expectErr    bool
		expectDenied bool
		wantErr      string
	}

	clusterObj := &clusterv1.ManagedCluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ManagedCluster",
			APIVersion: "cluster.open-cluster-management.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cluster",
		},
	}

	clusterData, _ := json.Marshal(clusterObj)

	clusterCreate := types.CloudEventsType{CloudEventsDataType: cluster.ManagedClusterEventDataType, SubResource: types.SubResourceSpec, Action: types.CreateRequestAction}
	clusterResync := types.CloudEventsType{CloudEventsDataType: cluster.ManagedClusterEventDataType, SubResource: types.SubResourceSpec, Action: types.ResyncRequestAction}
	leaseUpdate := types.CloudEventsType{CloudEventsDataType: lease.LeaseEventDataType, SubResource: types.SubResourceSpec, Action: types.UpdateRequestAction}
	csrCreate := types.CloudEventsType{CloudEventsDataType: csr.CSREventDataType, SubResource: types.SubResourceSpec, Action: types.CreateRequestAction}
	workStatusUpdate := types.CloudEventsType{CloudEventsDataType: payload.ManifestBundleEventDataType, SubResource: types.SubResourceStatus, Action: types.UpdateRequestAction}

	leaseObj := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: "test-cluster", Name: "test-lease"}}
	csrObj := func(labels map[string]string) *certificatesv1.CertificateSigningRequest {
		return &certificatesv1.CertificateSigningRequest{ObjectMeta: metav1.ObjectMeta{Name: "test-csr", Labels: labels}}
	}

	spoofedTypeRequest := newPublishRequest(t, clusterCreate, "test-cluster", clusterObj)
	spoofedTypeRequest.Event.Attributes["ce-type"] = &pbv1.CloudEventAttributeValue{
		Attr: &pbv1.CloudEventAttributeValue_CeString{CeString: workStatusUpdate.String()},
	}

	testCases := []testCase{
		{
			name: "allowed for cluster creation with resource name from payload",
			request: &pbv1.PublishRequest{
				Event: &pbv1.CloudEvent{
					SpecVersion: "1.0",
					Id:          "test-id",
					Source:      "test-source",
					Type:        "cluster.open-cluster-management.io.v1.managedclusters.spec.create_request",
					Attributes: map[string]*pbv1.CloudEventAttributeValue{
						"ce-clustername": {
							Attr: &pbv1.CloudEventAttributeValue_CeString{
								CeString: "test-cluster",
							},
						},
					},
					Data: &pbv1.CloudEvent_BinaryData{
						BinaryData: clusterData,
					},
				},
			},
			userCtx: func() context.Context {
				return context.WithValue(context.Background(), authn.ContextUserKey, "test-user")
			},
			allow: func(sar *authv1.SubjectAccessReview) bool {
				if sar.Spec.User != "test-user" {
					return false
				}

				if sar.Spec.ResourceAttributes.Group != clusterv1.SchemeGroupVersion.Group ||
					sar.Spec.ResourceAttributes.Resource != "managedclusters" ||
					sar.Spec.ResourceAttributes.Name != "test-cluster" ||
					sar.Spec.ResourceAttributes.Namespace != "test-cluster" {
					return false
				}

				if sar.Spec.ResourceAttributes.Verb != "create" {
					return false
				}

				return true
			},
			expectErr:    false,
			expectDenied: false,
		},
		{
			name: "denied for cluster creation",
			request: &pbv1.PublishRequest{
				Event: &pbv1.CloudEvent{
					SpecVersion: "1.0",
					Id:          "test-id",
					Source:      "test-source",
					Type:        "cluster.open-cluster-management.io.v1.managedclusters.spec.create_request",
					Attributes: map[string]*pbv1.CloudEventAttributeValue{
						"ce-clustername": {
							Attr: &pbv1.CloudEventAttributeValue_CeString{
								CeString: "test-cluster",
							},
						},
					},
					Data: &pbv1.CloudEvent_BinaryData{
						BinaryData: clusterData,
					},
				},
			},
			userCtx: func() context.Context {
				return context.WithValue(context.Background(), authn.ContextUserKey, "test-user")
			},
			allow: func(sar *authv1.SubjectAccessReview) bool {
				return false
			},
			expectErr:    true,
			expectDenied: true,
		},
		{
			name:         "denied for nil request",
			request:      nil,
			userCtx:      userContext("test-user"),
			expectErr:    true,
			expectDenied: true,
			wantErr:      "missing event in request",
		},
		{
			name:         "denied for request without event",
			request:      &pbv1.PublishRequest{},
			userCtx:      userContext("test-user"),
			expectErr:    true,
			expectDenied: true,
			wantErr:      "missing event in request",
		},
		{
			name:         "denied when ce-clustername is missing",
			request:      newPublishRequest(t, clusterCreate, "", clusterObj),
			userCtx:      userContext("test-user"),
			expectErr:    true,
			expectDenied: true,
			wantErr:      "ce-clustername",
		},
		{
			name:         "denied when a ce-type attribute spoofs the event type",
			request:      spoofedTypeRequest,
			userCtx:      userContext("test-user"),
			expectErr:    true,
			expectDenied: true,
			wantErr:      "carried in a dedicated field",
		},
		{
			name:         "denied when the managed cluster name does not match ce-clustername",
			request:      newPublishRequest(t, clusterCreate, "other-cluster", clusterObj),
			userCtx:      userContext("test-user"),
			expectErr:    true,
			expectDenied: true,
			wantErr:      `managed cluster name "test-cluster" does not match ce-clustername "other-cluster"`,
		},
		{
			name:         "denied when the lease namespace does not match ce-clustername",
			request:      newPublishRequest(t, leaseUpdate, "other-cluster", leaseObj),
			userCtx:      userContext("test-user"),
			expectErr:    true,
			expectDenied: true,
			wantErr:      `resource namespace "test-cluster" does not match ce-clustername "other-cluster"`,
		},
		{
			name:         "denied when the CSR is missing the cluster name label",
			request:      newPublishRequest(t, csrCreate, "test-cluster", csrObj(nil)),
			userCtx:      userContext("test-user"),
			expectErr:    true,
			expectDenied: true,
			wantErr:      "is missing the",
		},
		{
			name:         "denied when the CSR cluster name label does not match ce-clustername",
			request:      newPublishRequest(t, csrCreate, "test-cluster", csrObj(map[string]string{clusterv1.ClusterNameLabelKey: "other-cluster"})),
			userCtx:      userContext("test-user"),
			expectErr:    true,
			expectDenied: true,
			wantErr:      `CSR cluster name "other-cluster" does not match ce-clustername "test-cluster"`,
		},
		{
			name:         "denied when the CSR is published by another cluster's agent identity",
			request:      newPublishRequest(t, csrCreate, "test-cluster", csrObj(map[string]string{clusterv1.ClusterNameLabelKey: "test-cluster"})),
			userCtx:      userContext("system:open-cluster-management:other-cluster:agent-abc"),
			expectErr:    true,
			expectDenied: true,
			wantErr:      `is not allowed to act on cluster "test-cluster"`,
		},
		{
			name:    "allowed for CSR published by the cluster's agent identity",
			request: newPublishRequest(t, csrCreate, "test-cluster", csrObj(map[string]string{clusterv1.ClusterNameLabelKey: "test-cluster"})),
			userCtx: userContext("system:open-cluster-management:test-cluster:agent-abc"),
			allow: func(sar *authv1.SubjectAccessReview) bool {
				return sar.Spec.User == "system:open-cluster-management:test-cluster:agent-abc" &&
					sar.Spec.ResourceAttributes.Group == certificatesv1.GroupName &&
					sar.Spec.ResourceAttributes.Resource == "certificatesigningrequests" &&
					sar.Spec.ResourceAttributes.Namespace == "test-cluster" &&
					sar.Spec.ResourceAttributes.Verb == "create"
			},
		},
		{
			name:    "allowed for lease update in the cluster namespace",
			request: newPublishRequest(t, leaseUpdate, "test-cluster", leaseObj),
			userCtx: userContext("test-user"),
			allow: func(sar *authv1.SubjectAccessReview) bool {
				return sar.Spec.ResourceAttributes.Group == coordinationv1.GroupName &&
					sar.Spec.ResourceAttributes.Resource == "leases" &&
					sar.Spec.ResourceAttributes.Namespace == "test-cluster" &&
					sar.Spec.ResourceAttributes.Verb == "update"
			},
		},
		{
			name: "allowed for cluster resync without object metadata",
			request: newPublishRequest(t, clusterResync, "test-cluster", &genericpayload.ResourceVersionList{
				Versions: []genericpayload.ResourceVersion{{ResourceID: "test-cluster", ResourceVersion: 1}},
			}),
			userCtx: userContext("test-user"),
			allow: func(sar *authv1.SubjectAccessReview) bool {
				return sar.Spec.ResourceAttributes.Resource == "managedclusters" &&
					sar.Spec.ResourceAttributes.Name == "test-cluster" &&
					sar.Spec.ResourceAttributes.Namespace == "test-cluster" &&
					sar.Spec.ResourceAttributes.Verb == "list"
			},
		},
		{
			name:    "allowed for manifest bundle status update without object metadata",
			request: newPublishRequest(t, workStatusUpdate, "test-cluster", &payload.ManifestBundleStatus{Conditions: []metav1.Condition{}}),
			userCtx: userContext("test-user"),
			allow: func(sar *authv1.SubjectAccessReview) bool {
				return sar.Spec.ResourceAttributes.Group == workv1.GroupName &&
					sar.Spec.ResourceAttributes.Resource == "manifestworks" &&
					sar.Spec.ResourceAttributes.Subresource == "status" &&
					sar.Spec.ResourceAttributes.Namespace == "test-cluster" &&
					sar.Spec.ResourceAttributes.Verb == "update"
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()

			client.Fake.PrependReactor(
				"create",
				"subjectaccessreviews",
				func(action clienttesting.Action) (bool, runtime.Object, error) {
					createAction, ok := action.(clienttesting.CreateAction)
					if !ok {
						t.Fatalf("unexpected action %T", action)
					}

					sarObj := createAction.GetObject()
					sar, ok := sarObj.(*authv1.SubjectAccessReview)
					if !ok {
						t.Fatalf("unexpected object %T", sarObj)
					}

					if tc.allow == nil {
						t.Errorf("unexpected SubjectAccessReview %v, the request must be rejected before authorization", sar.Spec)
						return true, &authv1.SubjectAccessReview{}, nil
					}

					return true, &authv1.SubjectAccessReview{Status: authv1.SubjectAccessReviewStatus{Allowed: tc.allow(sar)}}, nil
				},
			)

			auth := NewSARAuthorizer(client)

			decision, authorizedCtx, err := auth.AuthorizeRequest(tc.userCtx(), tc.request)
			if tc.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tc.expectErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("expected error containing %q, got %v", tc.wantErr, err)
			}
			if !tc.expectErr && decision != authz.DecisionAllow {
				t.Errorf("expected DecisionAllow, got %v", decision)
			}
			if tc.expectDenied && decision != authz.DecisionDeny {
				t.Errorf("expected DecisionDeny, got %v", decision)
			}
			if authorizedCtx == nil {
				t.Fatal("expected a non-nil context to be returned")
			}
			if decision == authz.DecisionAllow {
				evt, ok := authz.AuthorizedEventFrom(authorizedCtx)
				if !ok || evt.Type() != tc.request.Event.Type {
					t.Errorf("expected the authorized event to be carried in the returned context, got %v", evt)
				}
			}
		})
	}
}

type fakeSubscribeStream struct {
	grpc.ServerStream
	req *pbv1.SubscriptionRequest
}

func (s *fakeSubscribeStream) Context() context.Context {
	return context.WithValue(context.Background(), authn.ContextUserKey, "test-user")
}

func (s *fakeSubscribeStream) RecvMsg(m any) error {
	msg, ok := m.(*pbv1.SubscriptionRequest)
	if !ok {
		return nil
	}
	msg.ClusterName = s.req.ClusterName
	msg.Source = s.req.Source
	msg.DataType = s.req.DataType
	return nil
}

func TestSARAuthorizeStream(t *testing.T) {
	cases := []struct {
		name         string
		req          *pbv1.SubscriptionRequest
		expectDenied bool
		wantErr      string
	}{
		{
			name:         "denied when the cluster name is missing",
			req:          &pbv1.SubscriptionRequest{Source: "test-source", DataType: lease.LeaseEventDataType.String()},
			expectDenied: true,
			wantErr:      "missing cluster name",
		},
		{
			name: "allowed when the cluster name is set",
			req:  &pbv1.SubscriptionRequest{ClusterName: "cluster1", Source: "test-source", DataType: lease.LeaseEventDataType.String()},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			client.Fake.PrependReactor(
				"create",
				"subjectaccessreviews",
				func(action clienttesting.Action) (bool, runtime.Object, error) {
					if tc.expectDenied {
						t.Errorf("unexpected SubjectAccessReview, the request must be rejected before authorization")
					}
					sar := action.(clienttesting.CreateAction).GetObject().(*authv1.SubjectAccessReview)
					allowed := sar.Spec.ResourceAttributes.Namespace == "cluster1" && sar.Spec.ResourceAttributes.Verb == "watch"
					return true, &authv1.SubjectAccessReview{Status: authv1.SubjectAccessReviewStatus{Allowed: allowed}}, nil
				},
			)

			auth := NewSARAuthorizer(client)
			decision, stream, err := auth.AuthorizeStream(
				context.Background(),
				&fakeSubscribeStream{req: tc.req},
				&grpc.StreamServerInfo{FullMethod: pbv1.CloudEventService_Subscribe_FullMethodName, IsServerStream: true},
			)

			if tc.expectDenied {
				if decision != authz.DecisionDeny {
					t.Errorf("expected DecisionDeny, got %v", decision)
				}
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if decision != authz.DecisionAllow {
				t.Fatalf("expected DecisionAllow, got %v", decision)
			}
			var got pbv1.SubscriptionRequest
			if err := stream.RecvMsg(&got); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.ClusterName != tc.req.ClusterName || got.DataType != tc.req.DataType {
				t.Errorf("expected the authorized stream to replay the subscription request, got %v", &got)
			}
		})
	}
}

func TestSARAuthorize(t *testing.T) {
	type testCase struct {
		name         string
		cluster      string
		resourceName string
		eventsType   types.CloudEventsType
		userCtx      func() context.Context
		allow        func(sar *authv1.SubjectAccessReview) bool
		expectErr    bool
	}

	testCases := []testCase{
		{
			name:    "allowed for cluster creation",
			cluster: "cluster1",
			eventsType: types.CloudEventsType{
				CloudEventsDataType: cluster.ManagedClusterEventDataType,
				SubResource:         types.SubResourceSpec,
				Action:              types.CreateRequestAction,
			},
			userCtx: func() context.Context {
				return context.WithValue(context.Background(), authn.ContextUserKey, "test")
			},
			allow: func(sar *authv1.SubjectAccessReview) bool {
				if sar.Spec.User != "test" {
					return false
				}

				if sar.Spec.ResourceAttributes.Group != clusterv1.SchemeGroupVersion.Group ||
					sar.Spec.ResourceAttributes.Resource != "managedclusters" ||
					sar.Spec.ResourceAttributes.Name != "cluster1" ||
					sar.Spec.ResourceAttributes.Namespace != "cluster1" {
					return false
				}

				if sar.Spec.ResourceAttributes.Verb != "create" {
					return false
				}

				return true
			},
			expectErr: false,
		},
		{
			name:         "allowed for service account token creation",
			cluster:      "cluster1",
			resourceName: "test-sa",
			eventsType: types.CloudEventsType{
				CloudEventsDataType: serviceaccount.TokenRequestDataType,
				SubResource:         types.SubResourceSpec,
				Action:              types.CreateRequestAction,
			},
			userCtx: func() context.Context {
				return context.WithValue(context.Background(), authn.ContextUserKey, "test")
			},
			allow: func(sar *authv1.SubjectAccessReview) bool {
				if sar.Spec.User != "test" {
					return false
				}

				if sar.Spec.ResourceAttributes.Group != "" ||
					sar.Spec.ResourceAttributes.Resource != "serviceaccounts" ||
					sar.Spec.ResourceAttributes.Subresource != "token" ||
					sar.Spec.ResourceAttributes.Name != "test-sa" ||
					sar.Spec.ResourceAttributes.Namespace != "cluster1" {
					return false
				}

				if sar.Spec.ResourceAttributes.Verb != "create" {
					return false
				}

				return true
			},
			expectErr: false,
		},
		{
			name:    "allowed for service account token subscription",
			cluster: "cluster1",
			eventsType: types.CloudEventsType{
				CloudEventsDataType: serviceaccount.TokenRequestDataType,
				SubResource:         types.SubResourceSpec,
				Action:              types.WatchRequestAction,
			},
			userCtx: func() context.Context {
				return context.WithValue(context.Background(), authn.ContextUserKey, "test")
			},
			allow: func(sar *authv1.SubjectAccessReview) bool {
				if sar.Spec.User != "test" {
					return false
				}

				if sar.Spec.ResourceAttributes.Group != "" ||
					sar.Spec.ResourceAttributes.Resource != "serviceaccounts" ||
					sar.Spec.ResourceAttributes.Subresource != "token" ||
					sar.Spec.ResourceAttributes.Namespace != "cluster1" {
					return false
				}

				if sar.Spec.ResourceAttributes.Verb != "subscribe" {
					return false
				}

				return true
			},
			expectErr: false,
		},
		{
			name:    "allowed for manifest status update",
			cluster: "cluster1",
			eventsType: types.CloudEventsType{
				CloudEventsDataType: payload.ManifestBundleEventDataType,
				SubResource:         types.SubResourceStatus,
				Action:              types.UpdateRequestAction,
			},
			userCtx: func() context.Context {
				return context.WithValue(context.Background(), authn.ContextGroupsKey, []string{"group1", "group2"})
			},
			allow: func(sar *authv1.SubjectAccessReview) bool {
				groups := sets.New(sar.Spec.Groups...)
				if !groups.Has("group2") {
					return false
				}

				if sar.Spec.ResourceAttributes.Group != workv1.SchemeGroupVersion.Group ||
					sar.Spec.ResourceAttributes.Resource != "manifestworks" ||
					sar.Spec.ResourceAttributes.Subresource != "status" ||
					sar.Spec.ResourceAttributes.Namespace != "cluster1" {
					return false
				}

				if sar.Spec.ResourceAttributes.Verb != "update" {
					return false
				}

				return true
			},
			expectErr: false,
		},
		{
			name:    "allowed for lease subscription",
			cluster: "cluster1",
			eventsType: types.CloudEventsType{
				CloudEventsDataType: lease.LeaseEventDataType,
				SubResource:         types.SubResourceSpec,
				Action:              types.WatchRequestAction,
			},
			userCtx: func() context.Context {
				return context.WithValue(context.Background(), authn.ContextUserKey, "test")
			},
			allow: func(sar *authv1.SubjectAccessReview) bool {
				if sar.Spec.User != "test" {
					return false
				}

				if sar.Spec.ResourceAttributes.Group != coordinationv1.SchemeGroupVersion.Group ||
					sar.Spec.ResourceAttributes.Resource != "leases" ||
					sar.Spec.ResourceAttributes.Namespace != "cluster1" {
					return false
				}

				if sar.Spec.ResourceAttributes.Verb != "watch" {
					return false
				}

				return true
			},
			expectErr: false,
		},
		{
			name:    "allowed for cluster resync",
			cluster: "cluster1",
			eventsType: types.CloudEventsType{
				CloudEventsDataType: cluster.ManagedClusterEventDataType,
				SubResource:         types.SubResourceSpec,
				Action:              types.ResyncRequestAction,
			},
			userCtx: func() context.Context {
				return context.WithValue(context.Background(), authn.ContextUserKey, "test")
			},
			allow: func(sar *authv1.SubjectAccessReview) bool {
				if sar.Spec.User != "test" {
					return false
				}

				if sar.Spec.ResourceAttributes.Group != clusterv1.SchemeGroupVersion.Group ||
					sar.Spec.ResourceAttributes.Resource != "managedclusters" ||
					sar.Spec.ResourceAttributes.Name != "cluster1" ||
					sar.Spec.ResourceAttributes.Namespace != "cluster1" {
					return false
				}

				if sar.Spec.ResourceAttributes.Verb != "list" {
					return false
				}

				return true
			},
			expectErr: false,
		},
		{
			name: "denied for cluster deletion",
			eventsType: types.CloudEventsType{
				CloudEventsDataType: cluster.ManagedClusterEventDataType,
				SubResource:         types.SubResourceSpec,
				Action:              types.DeleteRequestAction,
			},
			userCtx: func() context.Context {
				return context.WithValue(context.Background(), authn.ContextUserKey, "test")
			},
			allow: func(sar *authv1.SubjectAccessReview) bool {
				return false
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()

			client.Fake.PrependReactor(
				"create",
				"subjectaccessreviews",
				func(action clienttesting.Action) (bool, runtime.Object, error) {
					createAction, ok := action.(clienttesting.CreateAction)
					if !ok {
						t.Fatalf("unexpected action %T", action)
					}

					sarObj := createAction.GetObject()
					sar, ok := sarObj.(*authv1.SubjectAccessReview)
					if !ok {
						t.Fatalf("unexpected object %T", sarObj)
					}

					return true, &authv1.SubjectAccessReview{Status: authv1.SubjectAccessReviewStatus{Allowed: tc.allow(sar)}}, nil
				},
			)

			auth := NewSARAuthorizer(client)

			metaObj := metav1.ObjectMeta{}
			if tc.resourceName != "" {
				metaObj.Name = tc.resourceName
			}

			decision, err := auth.authorize(tc.userCtx(), tc.cluster, tc.eventsType, metaObj)
			if tc.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tc.expectErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tc.expectErr && decision != authz.DecisionAllow {
				t.Errorf("expected DecisionAllow, got %v", decision)
			}
			if tc.expectErr && decision != authz.DecisionDeny {
				t.Errorf("expected DecisionDeny, got %v", decision)
			}
		})
	}
}
