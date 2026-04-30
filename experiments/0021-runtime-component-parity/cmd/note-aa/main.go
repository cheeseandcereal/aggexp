// Command note-aa is the first consumer of the runtime/component
// substrate. It is a full aggregated apiserver for notes.aggexp.io
// — but the entire resource-specific implementation lives in a
// separate backend process reachable over gRPC. The binary compiled
// from this main.go knows nothing about Notes.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"

	"github.com/cheeseandcereal/aggexp/runtime/component"
)

func main() {
	opts := component.NewOptions()
	cmd := &cobra.Command{
		Use:   "note-aa",
		Short: "Generic aggexp component server; serves whatever resource its gRPC backend describes.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opts.Validate(); err != nil {
				return err
			}
			return component.Run(genericapiserver.SetupSignalContext(), opts)
		},
	}
	opts.AddFlags(cmd.Flags())
	logs.AddFlags(cmd.Flags())
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "note-aa exited with error")
		os.Exit(code)
	}
}
