// Package v2 is the second substrate promotion of the component-
// server pattern, consolidating the stateful-middleware-refinement
// arc (experiments 0022-0029). v2 lives alongside
// runtime/component (v1), which remains frozen for existing
// consumers (0013, 0017, 0018, 0019, 0020, 0021 class backends).
// New consumers should prefer v2.
//
// # Sub-packages
//
//   - proto/         — gRPC protocol with Validate/Mutate RPCs and
//                      watch_capability declaration.
//   - scheme/        — dynamic Scheme + typed wrapper (SSA seam).
//   - openapi/       — compose + Track B JSON-Schema-to-OpenAPI lift;
//                      v2-style "#/definitions/" refs (0024 fix);
//                      dynamic-install-friendly closure shape (0027).
//   - grpcbackend/   — the main REST adapter. Integrates
//                      MetadataStore, admission, unified RV
//                      authority, and initial-events-end BOOKMARK.
//   - httpbackend/   — HTTP/JSON + SSE client implementing the same
//                      Backend interface. Swappable with grpcbackend
//                      by flag (0026).
//   - metadatastore/ — CRD-backed KRM metadata store. Stitches
//                      metadata onto backend business data on every
//                      Get/List/Watch (0024).
//   - gc/            — GC reconciler for orphaned Records (0028).
//   - admission/     — CEL validation + JSONPath mutation engine.
//                      Composes additively with backend-RPC
//                      admission (0020/0029).
//   - multiplex/     — dynamic APIDefinition CRD reconciler. One
//                      process, many AAs, registered at runtime.
//                      Ships the APIDefinition CRD as a typed
//                      runtime.Object (0027).
//   - watch/         — transport-independent watch helpers (bookmark
//                      object builder; RV Authority).
//
// # Two consumer shapes
//
//  1. Single-AA. Call runtime/server.Options.Run with a
//     grpcbackend.REST wrapped in a runtime/group.Group. Full wire
//     parity with v1 (CRUD + list + watch + SSA + explain),
//     unconditional initial-events-end BOOKMARK, optional
//     MetadataStore + admission.Engine.
//
//  2. Multiplex. Instantiate a multiplex.Multiplex, attach it to a
//     built GenericAPIServer, install the APIDefinition CRD on the
//     host, and let the reconciler install groups dynamically. See
//     multiplex package doc for the known SSA/explain gap on
//     dynamically-installed groups.
//
// # What v2 closed vs v1
//
//   - "#/definitions/" refs, not "#/components/schemas/" (0024).
//     Strict OpenAPI consumers (ArgoCD) no longer reject the
//     aggregated schema.
//   - initial-events-end BOOKMARK emitted unconditionally (0011,
//     0025). `kubectl wait --for=jsonpath` passes.
//   - Unified RV authority (0025). Get/List/Watch all see the same
//     monotonic sequence; reflectors relist-with-RV cleanly.
//   - Dual transport (0026). gRPC + HTTP/SSE both first-class.
//   - Metadata-store + GC as substrate primitives (0024, 0028).
//   - Declarative admission (0029) additive to backend-RPC
//     admission (0020). One 422 wire shape for all denials.
//   - Dynamic-install-friendly OpenAPI closure (0027 cache-defeat
//     fix lands; V3 endpoint refresh + SSA typed-converter rebuild
//     remain known gaps — see multiplex package doc).
//
// # Known gaps in v2 alpha
//
//   - Dynamically-installed API groups (multiplex mode) do not
//     participate in /openapi/v3 or SSA typed-converter paths the
//     library pre-freezes at PrepareRun. CRUD + list + watch +
//     table rendering work; `kubectl explain` and `kubectl apply
//     --server-side` degrade. Statically-installed groups (single-
//     AA mode) have full parity.
//   - Schema evolution for ResourceMetadata is single-version only;
//     cross-version migration is not provided.
//   - Probe-vs-declared watch capability: the reconciler honors
//     the APIDefinition's watchCapability field; runtime probing
//     of "does the backend actually implement Watch" is not
//     wired in v2 alpha.
package v2
