// Package server wires the substrate's generic Options into this
// experiment's scheme + S3-backed backend.
package server

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/pflag"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0009-ack-aggregated-s3/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0009-ack-aggregated-s3/pkg/apiserver"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0009-ack-aggregated-s3/pkg/generated/openapi"
	"github.com/cheeseandcereal/aggexp/experiments/0009-ack-aggregated-s3/pkg/s3backend"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

type Options struct {
	*runtimeserver.Options

	AWSRegion     string
	AWSEndpoint   string
	AWSPathStyle  bool
	PollInterval  time.Duration
	NamePrefix    string
}

func NewOptions() *Options {
	return &Options{
		Options:      runtimeserver.NewOptions(),
		AWSPathStyle: true,
		PollInterval: 30 * time.Second,
	}
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = aggexpv1.GroupName
	o.Options.Title = "aggexp-s3"
	fs.StringVar(&o.AWSRegion, "aws-region", o.AWSRegion,
		"AWS region. Falls back to AWS_REGION env if empty.")
	fs.StringVar(&o.AWSEndpoint, "aws-endpoint-url", o.AWSEndpoint,
		"Override AWS S3 endpoint (for mocks / LocalStack). Empty means real AWS.")
	fs.BoolVar(&o.AWSPathStyle, "aws-s3-path-style", o.AWSPathStyle,
		"Use path-style S3 URLs. Required when pointing at a mock; usually fine against real AWS.")
	fs.DurationVar(&o.PollInterval, "s3-poll-interval", o.PollInterval,
		"How often to poll S3 for watch-event synthesis.")
	fs.StringVar(&o.NamePrefix, "bucket-name-prefix", o.NamePrefix,
		"If set, only buckets whose name starts with this prefix are projected.")
}

func (o *Options) Validate() error {
	var errs []error
	if err := o.Options.Validate(); err != nil {
		errs = append(errs, err)
	}
	return utilerrors.NewAggregate(errs)
}

func (o *Options) Run(ctx context.Context) error {
	o.Options.PolicyGroup = aggexpv1.GroupName

	// Build the S3 client. We honor AWS_REGION env if flag is empty.
	cfg, err := config.LoadDefaultConfig(ctx,
		func() config.LoadOptionsFunc {
			if o.AWSRegion != "" {
				return config.WithRegion(o.AWSRegion)
			}
			return func(_ *config.LoadOptions) error { return nil }
		}(),
	)
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(so *s3.Options) {
		if o.AWSEndpoint != "" {
			so.BaseEndpoint = aws.String(o.AWSEndpoint)
		}
		so.UsePathStyle = o.AWSPathStyle
	})

	backend := s3backend.New(s3backend.Options{
		Client:        client,
		DefaultRegion: cfg.Region,
		PollInterval:  o.PollInterval,
		NamePrefix:    o.NamePrefix,
	})

	buckets := runtimestorage.New(runtimestorage.Options{
		Backend:       backend,
		GroupResource: schema.GroupResource{Group: aggexpv1.GroupName, Resource: "buckets"},
	})
	backend.SetPublisher(buckets)

	g := &group.Group{
		GroupVersion:   aggexpv1.SchemeGroupVersion,
		Scheme:         apiserver.Scheme,
		Codecs:         apiserver.Codecs,
		ParameterCodec: apiserver.ParameterCodec,
		Resources:      map[string]rest.Storage{"buckets": buckets},
	}

	return o.Options.Run(
		ctx,
		"aggexp-s3-apiserver",
		runtimeserver.Input{
			Scheme:             apiserver.Scheme,
			Codecs:             apiserver.Codecs,
			OpenAPIDefinitions: generatedopenapi.GetOpenAPIDefinitions,
		},
		[]runtimeserver.GroupInstaller{g},
		map[string]runtimeserver.PostStartFunc{
			"s3-poller": func(hookCtx context.Context) error {
				backend.Start(hookCtx)
				go func() {
					<-hookCtx.Done()
					buckets.Shutdown()
				}()
				return nil
			},
		},
	)
}

var _ = func() *runtimeserver.Options { return (*runtimeserver.Options)(nil) }
var _ *genericapiserver.GenericAPIServer // keep import
