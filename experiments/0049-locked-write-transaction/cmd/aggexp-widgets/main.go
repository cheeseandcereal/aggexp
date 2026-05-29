// Command aggexp-widgets is experiment 0048: the capstone composed
// aggregated apiserver serving widgets.aggexp.io/v1 Widget (the
// 0046-generated types). The business body lives on a shared body CRD;
// the KRM metadata + authoritative resourceVersion + an embedded write
// lock live on a cluster-scoped metadata CR (0042/0043). Each replica
// runs informers on both CRDs; the metadata CR's host etcd RV is the
// single RV authority. Watch is per-watcher and identity-aware (0044);
// the read path reconciles against the backend as the source of truth
// for existence (0045). It composes every mechanism the multi-replica
// library arc validated in isolation.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0049-locked-write-transaction/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-widgets",
		Short: "aggexp experiment 0048: multi-replica library vertical slice (capstone)",
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
