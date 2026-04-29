// Command aggexp-files is experiment 0007: a filesystem-backed
// aggregated apiserver, built atop the runtime/ substrate.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-files",
		Short: "aggexp experiment 0007: filesystem aggregated apiserver on runtime/",
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
		fmt.Fprintln(os.Stderr, "aggexp-files exited with error")
		os.Exit(code)
	}
}
