// Command aggexp-repos is the experiment 0004 aggregated apiserver:
// it projects a GitHub user or org's repositories as a read-only
// Kubernetes resource type at aggexp.io/v1 Repo, backed by a
// polling GitHub REST client.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0004-github-driver-static-pat/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()

	cmd := &cobra.Command{
		Use:   "aggexp-repos",
		Short: "aggexp experiment 0004: GitHub repos as aggregated API",
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
		fmt.Fprintln(os.Stderr, "aggexp-repos exited with error")
		os.Exit(code)
	}
}
