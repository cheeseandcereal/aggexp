// Package componentv2pb holds the v2 component-server gRPC
// protocol definitions. The .proto file and generated bindings
// encode the wire contract between the middleware
// (runtime/component/v2) and a language-agnostic backend.
//
// v2 differences from v1 (runtime/component/proto):
//
//   - Validate + Mutate RPCs (from 0020), opt-in per schema flag.
//   - watch_capability declaration in GetSchemaResponse (0025).
//   - schema_is_openapi flag so Track B (plain JSON Schema,
//     synthesized by the middleware) and Track A (full OpenAPI)
//     are both supported without ambiguity (0023).
//   - AdmissionCause list on ValidateResponse so backends can
//     emit multi-cause 422 responses using the unified wire shape
//     identified in 0020/0029.
//
// Transport is swappable: runtime/component/v2/grpcbackend is the
// gRPC implementation and runtime/component/v2/httpbackend is the
// HTTP/JSON+SSE implementation of the same logical contract (0026).
package componentv2pb
