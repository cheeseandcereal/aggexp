// Command aggexp-widgets is the experiment 0011 aggregated apiserver:
// it exposes aggexp.io/v1 Widget resources backed by an async HTTP
// mock. Create / Update return immediately with phase=Provisioning;
// a poll loop drives watch events as the mock transitions to Ready.
//
// This probes the sync-vs-async backend boundary flagged by
// FINDINGS/0009-ack-aggregated-s3.md: what specifically breaks when
// an AA's backend has minute-scale async provisioning?
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0011-async-backend-sim/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-widgets",
		Short: "aggexp experiment 0011: async-backend-sim (Widget)",
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
