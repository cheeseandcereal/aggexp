// Command aggexp-authz is the experiment 0003 aggregated apiserver,
// a fork of 0002 that wires a custom identity-based authorizer which
// consults an external HTTP policy service on every request.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0003-custom-authorizer-external-policy/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()

	cmd := &cobra.Command{
		Use:   "aggexp-authz",
		Short: "aggexp experiment 0002: in-memory aggregated apiserver",
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
	ctx := context.Background()
	cmd := newCommand()
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "aggexp-authz exited with error")
		os.Exit(code)
	}
	_ = ctx
}
