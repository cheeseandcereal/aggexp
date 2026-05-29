// Command oapigen is the experiment 0046 OpenAPI-first code generator.
// It consumes a hand-authored OpenAPI v3 document plus a small config
// and emits the Go artifacts an aggregated API needs: typed structs,
// deepcopy, dual-version scheme registration with identity conversions,
// field-label conversions, and openapi-gen-shaped definitions.
//
// Stages: parse OpenAPI v3 -> resolve intra-document $refs (external
// refs rejected) -> identify spec/status components from config ->
// synthesize the <Kind>/<Kind>List shape -> emit Go.
//
// Output is reproducible: same input + config + tool version yields
// byte-identical files (no clock, hostname, or randomness). The input
// OpenAPI SHA-256 is recorded in the generated doc.go.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	var configPath, openapiPath string
	cmd := &cobra.Command{
		Use:   "oapigen",
		Short: "OpenAPI v3 -> Go aggregated-API code generator (experiment 0046)",
		RunE: func(_ *cobra.Command, _ []string) error {
			return run(configPath, openapiPath)
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "path to oapigen.yaml config")
	cmd.Flags().StringVar(&openapiPath, "openapi", "", "path to the OpenAPI v3 document")
	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagRequired("openapi")

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "oapigen:", err)
		os.Exit(1)
	}
}

func run(configPath, openapiPath string) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	openapiBytes, err := os.ReadFile(openapiPath)
	if err != nil {
		return fmt.Errorf("read openapi: %w", err)
	}
	model, err := parse(openapiBytes, cfg)
	if err != nil {
		return err
	}
	if err := emit(model); err != nil {
		return err
	}
	fmt.Printf("oapigen: wrote %s (%d structs, %d enums) sha256=%s\n",
		cfg.OutputDir, len(model.Structs), len(model.Enums), model.OpenAPISHA256[:12])
	return nil
}
