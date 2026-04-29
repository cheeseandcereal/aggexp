// Command aggexp-s3 is the experiment 0009 aggregated apiserver:
// it exposes aggexp.io/v1 Bucket resources backed by the AWS S3 API
// with NO local state store. This is the ACK-as-aggregated-API
// thought experiment: AWS is the source of truth; reads are live;
// writes are direct API calls; watch is re-implemented as a poll
// loop that diffs S3 state and emits events.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/experiments/0009-ack-aggregated-s3/pkg/server"
)

func newCommand() *cobra.Command {
	opts := server.NewOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-s3",
		Short: "aggexp experiment 0009: ACK-as-aggregated-API (S3 Bucket)",
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
		fmt.Fprintln(os.Stderr, "aggexp-s3 exited with error")
		os.Exit(code)
	}
}
