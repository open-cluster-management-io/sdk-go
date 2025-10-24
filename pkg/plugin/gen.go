package plugin

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative rollout.proto

// Install the protoc-gen-go and protoc-gen-go-grpc plugins before generating the code.
//
// $ go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
// $ go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
