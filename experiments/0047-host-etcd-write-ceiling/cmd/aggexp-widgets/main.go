// Command aggexp-widgets is experiment 0047: the composed
// metadata-CR core (0042) + embedded per-object lock with emission
// filtering (0043) + per-watcher identity-aware watch (0044) in ONE
// aggregated apiserver binary. This experiment introduces no new
// mechanism; it composes 0043 and 0044 so the combined host-etcd
// write rate (observed-hash pump + lock acquire/release/renewal +
// per-watcher poll) can be measured and the scaling ceiling located.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-widgets",
		Short: "aggexp experiment 0047: composed embedded-lock + per-watcher AA for host-etcd write-ceiling measurement",
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
