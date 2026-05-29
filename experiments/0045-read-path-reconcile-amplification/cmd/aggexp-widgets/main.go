// Command aggexp-widgets is experiment 0045: an aggregated apiserver
// serving aggexp.io/v1 Widget where the BACKEND IS THE SOURCE OF
// TRUTH FOR EXISTENCE. The business body lives on a shared body CRD;
// the KRM metadata + authoritative RV live on a metadata CR. The read
// path reconciles the metadata store against the backend INLINE on
// every Get and List — adopting unknown backend objects and
// collecting orphan records (minAge grace) — and a periodic sweep
// runs the same reconcile. There is no tolerant-Get: a backend 404 is
// a 404 regardless of finalizers. Backend-call counters on a debug
// endpoint measure the read amplification this costs.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-widgets",
		Short: "aggexp experiment 0045: backend-as-source-of-truth read-path reconcile + amplification",
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
