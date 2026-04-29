// Command aggexp-repos is the experiment 0006 aggregated apiserver:
// it projects GitHub-shaped repositories as a read-only Kubernetes
// resource type at aggexp.io/v1 Repo. Every Get/List/Watch is
// served per-caller by exchanging the caller's user.Info for a
// short-lived token at an identity broker, then using that token
// to call a (mock) GitHub REST API.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0006-identity-broker-github-app/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()

	cmd := &cobra.Command{
		Use:   "aggexp-repos",
		Short: "aggexp experiment 0006: GitHub repos via identity broker",
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
