package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the generator configuration (oapigen.yaml). It tells the
// generator how to lift the plain OpenAPI component schemas into a
// Kubernetes resource package.
type Config struct {
	Group     string `yaml:"group"`
	Version   string `yaml:"version"`
	Kind      string `yaml:"kind"`
	Plural    string `yaml:"plural"`
	Singular  string `yaml:"singular"`
	Namespaced bool  `yaml:"namespaced"`

	SpecComponent   string `yaml:"specComponent"`
	StatusComponent string `yaml:"statusComponent"`

	ListSelectableFields []string `yaml:"listSelectableFields"`

	OutputDir     string `yaml:"outputDir"`
	Package       string `yaml:"package"`
	GoPackagePath string `yaml:"goPackagePath"`
}

func loadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) validate() error {
	var missing []string
	if c.Group == "" {
		missing = append(missing, "group")
	}
	if c.Version == "" {
		missing = append(missing, "version")
	}
	if c.Kind == "" {
		missing = append(missing, "kind")
	}
	if c.Plural == "" {
		missing = append(missing, "plural")
	}
	if c.OutputDir == "" {
		missing = append(missing, "outputDir")
	}
	if c.Package == "" {
		missing = append(missing, "package")
	}
	if c.GoPackagePath == "" {
		missing = append(missing, "goPackagePath")
	}
	if len(missing) > 0 {
		return fmt.Errorf("config missing required fields: %s", strings.Join(missing, ", "))
	}
	if c.Singular == "" {
		c.Singular = strings.ToLower(c.Kind)
	}
	return nil
}

// listKind is "<Kind>List".
func (c *Config) listKind() string { return c.Kind + "List" }
