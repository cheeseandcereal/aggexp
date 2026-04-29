// Package server provides the generic, etcd-less aggregated-apiserver
// plumbing shared by every experiment in this repo: the hand-rolled
// Options struct (SecureServing + DelegatingAuth* + Audit + Features
// + CoreAPI, deliberately no EtcdOptions), its AddFlags / Validate /
// Config wiring, and a Run that installs one or more API groups into
// the generic apiserver.
//
// The shape mirrors the metrics-server pattern: composing
// genericoptions directly rather than pulling in RecommendedOptions
// (which assumes etcd). Experiments can still extend the Options
// struct by embedding it.
//
// This package does not know about specific resources. Resource
// wiring lives in the runtime/group package.
package server
