// Command aggexp-widgets is experiment 0042: an aggregated apiserver
// serving aggexp.io/v1 Widget where the business body (spec+status)
// lives in an in-memory backend and the KRM metadata + the
// authoritative resourceVersion live on a cluster-scoped metadata CR
// (resourcemetadatas.widgetmeta.aggexp.io/v1) on the host cluster.
// Every served object's resourceVersion is the host etcd RV of its
// metadata CR. Multiple replicas each run an informer on the metadata
// CRD, so all replicas observe the same monotonic RV stream and serve
// consistent Get/List/Watch with cross-replica resume-by-RV.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0042-metadata-cr-rv-authority/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-widgets",
		Short: "aggexp experiment 0042: metadata-CR RV authority over the stitched store",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opts.Validate(); err != nil {
				return err
			}
			ctx := genericapiserver.SetupSignalContext()
			return opts.Run(ctx)
		},
	}
	opts.AddFlags(cmd.Flags())
	logs.AddFlags(cmd.Flags())
	return cmd
}

func main() {
	cmd := newCommand()
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "aggexp-widgets exited with error")
		os.Exit(code)
	}
}
