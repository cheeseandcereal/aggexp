// Command aggexp-widgets is experiment 0010: an aggregated
// apiserver that serves widgets.aggexp.io/v1 by forwarding every
// storage operation to a CRD on the host kube-apiserver. The AA holds
// no state of its own; the CRD carries the ObjectMeta bookkeeping
// (managedFields, finalizers, ownerReferences, labels, annotations)
// that a fully-stateless AA cannot. Demonstrates a facade that
// transforms + identity-filters without touching the backing store.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0010-etcd-crd-facade-with-ssa/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-widgets",
		Short: "aggexp experiment 0010: etcd-CRD-facade with SSA",
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
