package util

import (
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
)

func NewGRPCAgentOptions(brokerURL string) *grpc.GRPCOptions {
	return newGRPCOptions(brokerURL)
}

func newGRPCOptions(brokerURL string) *grpc.GRPCOptions {
	return &grpc.GRPCOptions{
		URL: brokerURL,
	}
}
