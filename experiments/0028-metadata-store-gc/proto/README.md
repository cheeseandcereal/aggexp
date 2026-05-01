# Proto

This directory carries a verbatim copy of `runtime/component/proto/backend.proto`.

0024 did not extend the proto: the backend continues to serve
business data only. The middleware is responsible for KRM metadata
overlay via the `component/metastore` package and a shared
cluster-scoped `ResourceMetadata` CRD on the host.

The runtime substrate's generated Go bindings
(`runtime/component/proto/backend.pb.go`) are imported directly;
no per-experiment codegen lives in `gen/` (the directory is kept
empty for structural consistency with future experiments that may
need it).
