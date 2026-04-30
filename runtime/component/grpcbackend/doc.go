// Package grpcbackend implements a rest.Storage that proxies to a
// gRPC backend speaking runtime/component/proto's Backend service.
//
// It is the inverse of runtime/storage's adapter: where that package
// wraps a typed in-process Go Backend, this one wraps a network call
// to a separate process (in any language) that doesn't know about
// k8s.io/apiserver.
//
// Objects on the wire are JSON bytes. Inside the component server
// they decode to either *unstructured.Unstructured (the default) or
// *scheme.Object (when runtime/component/scheme's typed-wrapper mode
// is used). The typed-wrapper mode is required for Server-Side
// Apply; see runtime/component/scheme's package doc.
//
// The REST owns a monotonic resourceVersion counter and a
// watch.Broadcaster. Watch events from the backend's long-lived
// Watch stream are fanned out through the broadcaster to every
// kubectl watcher; the RV is stamped on each event as it goes.
// StartUpstreamWatch must be called from a post-start hook to open
// the upstream stream with retry.
//
// Compile-time interface assertions appear at the bottom of rest.go.
package grpcbackend
