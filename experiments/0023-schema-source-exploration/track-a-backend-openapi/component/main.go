// Command note-aa-0023a is the Track A component server for
// experiment 0023. Backend-ships-full-OpenAPI: this binary is a
// thin cobra/component.Run() shim, identical to 0021's note-aa.
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
		Use:   "note-aa-0023a",
		Short: "Track A (backend-ships-openapi) component server for 0023.",
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
		fmt.Fprintln(os.Stderr, "note-aa-0023a exited with error")
		os.Exit(code)
	}
}
