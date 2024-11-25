package util

import (
	"time"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
)

func NewGRPCAgentOptions(brokerURL string) *grpc.GRPCOptions {
	return newGRPCOptions(brokerURL)
}

func newGRPCOptions(brokerURL string) *grpc.GRPCOptions {
	return &grpc.GRPCOptions{
		URL: brokerURL,
		KeepAliveOptions: grpc.KeepAliveOptions{
			Enable:              true,
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		},
	}
}
