// Package component is the substrate shape for running a
// deployable, schema-dynamic Kubernetes "component server" that
// proxies CRUD to a resource-specific "backend" over gRPC.
//
// # When to use this package
//
// The substrate gives you TWO ways to build an aggregated API:
//
//   - The library approach (runtime/server + runtime/storage +
//     runtime/group): a single Go binary links against the substrate
//     and implements runtime/storage.Backend for its resource. Best
//     for Go-native backends, typed Go models with codegen'd
//     deepcopy, and experiments where the backend is a library-level
//     consumer. Used by 0002, 0007, 0009, 0010, 0011.
//
//   - The component approach (this package): a generic component
//     server binary speaks the Kubernetes wire contract for any
//     resource; the resource-specific logic lives in a separate
//     backend process (possibly in another language) behind the
//     runtime/component/proto.Backend gRPC service. Best for
//     polyglot backends, for amortizing the apiserver wiring cost
//     across many backends, or for cases where the backend must run
//     in a different security/trust domain than the apiserver. Used
//     by 0013, 0017, 0018, 0021.
//
// Both approaches share runtime/server, runtime/authz,
// runtime/group, and the same Options/Run contract. The choice is
// how the resource is implemented, not how it is served.
//
// # Component-mode architecture
//
//	kubectl --(HTTPS+mTLS)--> kube-apiserver --(mTLS+headers)-->
//	  component server [this package]
//	    │
//	    │  dials once at startup
//	    ▼
//	  backend process [runtime/component/proto.BackendServer]
//	    • GetSchema (GVR, Kind, OpenAPI, columns, writable, SSA, ...)
//	    • Get / List / Create / Update / Apply / Delete
//	    • Watch (server-streaming)
//
// The component server is stateless beyond a monotonic
// resourceVersion counter and a watch broadcaster. Persistence lives
// entirely on the backend side.
//
// # Typical main()
//
//	package main
//
//	import (
//	    "github.com/cheeseandcereal/aggexp/runtime/component"
//	    genericapiserver "k8s.io/apiserver/pkg/server"
//	    "k8s.io/component-base/cli"
//	    "github.com/spf13/cobra"
//	)
//
//	func main() {
//	    opts := component.NewOptions()
//	    cmd := &cobra.Command{
//	        Use: "my-aa",
//	        RunE: func(cmd *cobra.Command, _ []string) error {
//	            if err := opts.Validate(); err != nil {
//	                return err
//	            }
//	            return component.Run(
//	                genericapiserver.SetupSignalContext(), opts)
//	        },
//	    }
//	    opts.AddFlags(cmd.Flags())
//	    _ = cli.Run(cmd)
//	}
//
// See experiments/0021-runtime-component-parity for a concrete
// consumer.
package component
