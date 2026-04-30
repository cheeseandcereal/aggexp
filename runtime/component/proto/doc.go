// Package componentpb holds the committed gRPC bindings for
// runtime/component/proto/backend.proto.
//
// Generated with:
//
//	protoc --go_out=. --go_opt=paths=source_relative \
//	  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
//	  runtime/component/proto/backend.proto
//
// The bindings are committed so consumers of the substrate don't
// need a local protoc toolchain. Do not hand-edit backend.pb.go or
// backend_grpc.pb.go — edit the .proto file and regenerate.
//
// The protocol is designed to be stable across minor versions.
// Field additions are backward-compatible; RPC additions are
// wire-compatible as long as backends leave new flags default.
package componentpb
