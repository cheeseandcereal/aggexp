// Package s3backend implements a runtime/storage.Backend +
// WritableBackend that treats AWS S3 as the source of truth for
// Bucket resources. No local persistence. No etcd.
//
// Reads (Get / List) are **live** calls against the S3 API. The AA
// has no authoritative state of its own; the cache that exists below
// serves exactly one purpose — computing diffs for the watch event
// stream. Clients reading at different moments may observe different
// states depending on S3's eventual-consistency behavior. For the
// MVP against a mock this is invisible; against real AWS it would
// surface.
//
// Writes (Create / Update / Delete) make the corresponding S3 API
// call and then re-read to produce the response object. Partial
// failures (e.g. CreateBucket succeeds but PutBucketTagging fails)
// are surfaced as errors with the bucket left in whatever state S3
// actually reached. The "controller-like" reconciliation loop that
// would retry PutBucketTagging later is **absent by design**: this
// experiment's thesis is that a CRD+controller model's retry logic
// is not necessary if the AA is honest about failure modes. Clients
// that want retries can retry the request.
//
// Watch is re-implemented via a polling loop that calls
// ListBuckets (+ GetBucketTagging per bucket where relevant), diffs
// against the previous observation, and emits Added/Modified/Deleted
// events via the runtime/storage.Publisher.
package s3backend

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0009-ack-aggregated-s3/pkg/apis/aggexp"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Options configures the Backend.
type Options struct {
	// Client is the S3 client. Required.
	Client *s3.Client
	// DefaultRegion is the region to use when a Bucket spec does
	// not set one. Typical value: the client's region.
	DefaultRegion string
	// PollInterval is how often the watch-feed loop refreshes
	// against S3. Defaults to 30s if zero.
	PollInterval time.Duration
	// NamePrefix, if non-empty, filters which buckets the
	// experiment "sees". A bucket matches iff its name has this
	// prefix. Useful in shared AWS accounts so the AA does not
	// project every unrelated bucket. Empty means all.
	NamePrefix string
}

// Backend is the runtime/storage.Backend + WritableBackend
// implementation backed by S3.
type Backend struct {
	client        *s3.Client
	defaultRegion string
	interval      time.Duration
	prefix        string

	// uids persists UIDs across polls so clients see stable
	// object identity within the AA's lifetime. Not etcd; still
	// process-local (0004 pod-restart-amnesia applies).
	mu    sync.RWMutex
	uids  map[string]types.UID
	seen  map[string]*aggexp.Bucket // last observed per bucket; diff source
	ready bool

	publisher runtimestorage.Publisher
}

// New constructs a Backend. SetPublisher must be called before Start.
func New(opts Options) *Backend {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 30 * time.Second
	}
	return &Backend{
		client:        opts.Client,
		defaultRegion: opts.DefaultRegion,
		interval:      opts.PollInterval,
		prefix:        opts.NamePrefix,
		uids:          map[string]types.UID{},
		seen:          map[string]*aggexp.Bucket{},
	}
}

// SetPublisher wires the adapter's publisher for watch events.
func (b *Backend) SetPublisher(p runtimestorage.Publisher) { b.publisher = p }

// Start launches the poll loop.
func (b *Backend) Start(ctx context.Context) {
	go func() {
		b.pollOnce(ctx)
		t := time.NewTicker(b.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.pollOnce(ctx)
			}
		}
	}()
}

// ---- runtime/storage.Backend identity ----

func (b *Backend) New() runtime.Object     { return &aggexp.Bucket{} }
func (b *Backend) NewList() runtime.Object { return &aggexp.BucketList{} }
func (b *Backend) Kind() string            { return "Bucket" }
func (b *Backend) SingularName() string    { return "bucket" }
func (b *Backend) NamespaceScoped() bool   { return false }

// ---- Get: LIVE read from S3 ----

func (b *Backend) Get(ctx context.Context, u user.Info, name string) (runtime.Object, error) {
	logUser("get", u, "name", name)
	if !b.matchesPrefix(name) {
		return nil, apierrors.NewNotFound(aggexp.Resource("buckets"), name)
	}

	head, err := b.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
	if err != nil {
		if isNotFoundErr(err) {
			return nil, apierrors.NewNotFound(aggexp.Resource("buckets"), name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("HeadBucket %s: %w", name, err))
	}

	tags, err := b.getTags(ctx, name)
	if err != nil {
		// Not fatal for a Get; surface empty tags + note in status.
		klog.V(2).InfoS("get-tags-failed", "name", name, "err", err)
		tags = map[string]string{}
	}

	obj := &aggexp.Bucket{
		Spec: aggexp.BucketSpec{
			Region: aws.ToString(head.BucketRegion),
			Tags:   tags,
		},
		Status: aggexp.BucketStatus{
			Region:     aws.ToString(head.BucketRegion),
			ObservedAt: metav1.NewTime(time.Now()),
			Phase:      "Ready",
		},
	}
	obj.Name = name
	obj.TypeMeta.Kind = "Bucket"
	obj.TypeMeta.APIVersion = "aggexp.io/v1"

	// Assign stable UID if we have seen this bucket before; else
	// mint a new one. CreationTimestamp from our records if we have
	// it; else derived from S3 on next list.
	b.applyIdentity(obj)
	return obj, nil
}

// ---- List: LIVE read from S3 ----

func (b *Backend) List(ctx context.Context, u user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	logUser("list", u)
	out, err := b.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("ListBuckets: %w", err))
	}

	list := &aggexp.BucketList{Items: make([]aggexp.Bucket, 0, len(out.Buckets))}
	now := metav1.NewTime(time.Now())

	for _, gb := range out.Buckets {
		name := aws.ToString(gb.Name)
		if !b.matchesPrefix(name) {
			continue
		}
		obj := aggexp.Bucket{
			Spec: aggexp.BucketSpec{
				Region: aws.ToString(gb.BucketRegion),
			},
			Status: aggexp.BucketStatus{
				Region:     aws.ToString(gb.BucketRegion),
				ObservedAt: now,
				Phase:      "Ready",
			},
		}
		obj.Name = name
		obj.TypeMeta.Kind = "Bucket"
		obj.TypeMeta.APIVersion = "aggexp.io/v1"
		if gb.CreationDate != nil {
			cd := metav1.NewTime(*gb.CreationDate)
			obj.Status.CreationDate = &cd
			// Use the same value for ObjectMeta.CreationTimestamp so
			// the kubectl display is consistent.
			obj.CreationTimestamp = cd
		}
		b.applyIdentity(&obj)
		list.Items = append(list.Items, obj)
	}
	// Deterministic order.
	sort.Slice(list.Items, func(i, j int) bool {
		return list.Items[i].Name < list.Items[j].Name
	})
	return list, nil
}

// ---- Create: S3 CreateBucket + optional PutBucketTagging ----

func (b *Backend) Create(ctx context.Context, u user.Info, obj runtime.Object) (runtime.Object, error) {
	bucket, ok := obj.(*aggexp.Bucket)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("expected Bucket, got %T", obj))
	}
	name := bucket.Name
	if name == "" {
		return nil, apierrors.NewBadRequest("metadata.name is required (and becomes the S3 bucket name)")
	}
	logUser("create", u, "name", name)

	region := bucket.Spec.Region
	if region == "" {
		region = b.defaultRegion
	}

	input := &s3.CreateBucketInput{Bucket: aws.String(name)}
	if region != "" && region != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}

	_, err := b.client.CreateBucket(ctx, input)
	if err != nil {
		var bae *s3types.BucketAlreadyExists
		var baou *s3types.BucketAlreadyOwnedByYou
		switch {
		case errors.As(err, &bae):
			return nil, apierrors.NewAlreadyExists(aggexp.Resource("buckets"), name)
		case errors.As(err, &baou):
			// Already yours. Idempotent success -- fall through to
			// the tag write.
		default:
			return nil, apierrors.NewInternalError(fmt.Errorf("CreateBucket %s: %w", name, err))
		}
	}

	// Apply tags if any. We do this unconditionally (including when
	// the set is empty — an S3 bucket with no tag set is distinct
	// from one with an empty tag set). Matching up to PutBucketTagging's
	// requirement of at least one tag by skipping the call when
	// empty.
	if len(bucket.Spec.Tags) > 0 {
		if err := b.putTags(ctx, name, bucket.Spec.Tags); err != nil {
			// Partial failure — surface the error. The bucket now
			// exists; a retry or a subsequent apply would complete
			// the work. See package doc for the design rationale.
			return nil, apierrors.NewInternalError(fmt.Errorf(
				"bucket %s created but tagging failed: %w (retry apply to complete)", name, err))
		}
	}

	// Live-read for the response.
	result, err := b.Get(ctx, u, name)
	if err != nil {
		return nil, err
	}
	// Preserve any library-managed metadata (managedFields, labels,
	// annotations) the caller handed us. See preserveManagedMeta.
	merged := result.(*aggexp.Bucket).DeepCopy()
	preserveManagedMeta(merged, bucket)

	// Inject into the watch stream even though we'll see it on the
	// next poll anyway — eager publish means clients watching don't
	// wait a poll interval for their own write's event.
	if b.publisher != nil {
		b.publisher.PublishAdded(merged.DeepCopy())
	}
	return merged, nil
}

// ---- Update: only Tags are mutable on S3 buckets for us ----

func (b *Backend) Update(ctx context.Context, u user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	bucket, ok := obj.(*aggexp.Bucket)
	if !ok {
		return nil, false, apierrors.NewBadRequest(fmt.Sprintf("expected Bucket, got %T", obj))
	}
	if bucket.Name == "" {
		bucket.Name = name
	}
	if bucket.Name != name {
		return nil, false, apierrors.NewBadRequest(fmt.Sprintf("body name %q != path name %q", bucket.Name, name))
	}
	logUser("update", u, "name", name)

	// Check existence.
	_, err := b.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
	exists := err == nil
	if err != nil && !isNotFoundErr(err) {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("HeadBucket %s: %w", name, err))
	}

	created := false
	if !exists {
		if !forceAllowCreate {
			return nil, false, apierrors.NewNotFound(aggexp.Resource("buckets"), name)
		}
		if _, err := b.Create(ctx, u, obj); err != nil {
			return nil, false, err
		}
		created = true
	} else {
		if len(bucket.Spec.Tags) > 0 {
			if err := b.putTags(ctx, name, bucket.Spec.Tags); err != nil {
				return nil, false, apierrors.NewInternalError(fmt.Errorf("PutBucketTagging %s: %w", name, err))
			}
		}
	}

	// Fetch the current live state from AWS, then preserve the
	// library-managed ObjectMeta fields the caller handed us (uid,
	// creationTimestamp if set, managedFields, labels, annotations).
	// Without this step, server-side apply appears to succeed but
	// managedFields vanish from the response because our Get does
	// not know about them.
	result, err := b.Get(ctx, u, name)
	if err != nil {
		return nil, false, err
	}
	merged := result.(*aggexp.Bucket).DeepCopy()
	preserveManagedMeta(merged, bucket)

	if b.publisher != nil {
		if created {
			b.publisher.PublishAdded(merged.DeepCopy())
		} else {
			b.publisher.PublishModified(merged.DeepCopy())
		}
	}
	return merged, created, nil
}

// ---- Delete: S3 DeleteBucket ----

func (b *Backend) Delete(ctx context.Context, u user.Info, name string) (runtime.Object, bool, error) {
	logUser("delete", u, "name", name)
	// Snapshot before delete so we have an object to return.
	prior, _ := b.Get(ctx, u, name)

	_, err := b.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
	if err != nil {
		if isNotFoundErr(err) {
			return nil, false, apierrors.NewNotFound(aggexp.Resource("buckets"), name)
		}
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "BucketNotEmpty" {
			return nil, false, apierrors.NewConflict(aggexp.Resource("buckets"), name, fmt.Errorf("bucket %s not empty", name))
		}
		return nil, false, apierrors.NewInternalError(fmt.Errorf("DeleteBucket %s: %w", name, err))
	}

	// Clear identity state.
	b.mu.Lock()
	delete(b.uids, name)
	delete(b.seen, name)
	b.mu.Unlock()

	if prior == nil {
		// Rare: the bucket existed at delete time but HeadBucket
		// raced us. Construct a minimal tombstone.
		prior = &aggexp.Bucket{Spec: aggexp.BucketSpec{}}
		(prior.(*aggexp.Bucket)).Name = name
		prior.(*aggexp.Bucket).TypeMeta.Kind = "Bucket"
		prior.(*aggexp.Bucket).TypeMeta.APIVersion = "aggexp.io/v1"
	}
	if b.publisher != nil {
		b.publisher.PublishDeleted(prior.(*aggexp.Bucket).DeepCopy())
	}
	return prior, true, nil
}

// ---- Table ----

func (b *Backend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name", Description: "S3 bucket name."},
		{Name: "Region", Type: "string", Description: "AWS region."},
		{Name: "Tags", Type: "integer", Description: "Number of tags."},
		{Name: "Created", Type: "date", Description: "Time since creation on AWS."},
		{Name: "Phase", Type: "string", Description: "Coarse state."},
	}
}

func (b *Backend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	row := func(bkt *aggexp.Bucket) metav1.TableRow {
		age := "<unknown>"
		if bkt.Status.CreationDate != nil {
			age = translateTimestampSince(*bkt.Status.CreationDate)
		}
		return metav1.TableRow{
			Cells: []interface{}{
				bkt.Name,
				bkt.Spec.Region,
				int64(len(bkt.Spec.Tags)),
				age,
				bkt.Status.Phase,
			},
			Object: runtime.RawExtension{Object: bkt},
		}
	}
	switch v := obj.(type) {
	case *aggexp.Bucket:
		return []metav1.TableRow{row(v)}, nil
	case *aggexp.BucketList:
		rs := make([]metav1.TableRow, 0, len(v.Items))
		for i := range v.Items {
			rs = append(rs, row(&v.Items[i]))
		}
		return rs, nil
	}
	return nil, fmt.Errorf("unexpected object %T", obj)
}

// ---- poll loop: watch event synthesis ----

func (b *Backend) pollOnce(ctx context.Context) {
	t0 := time.Now()
	result, err := b.List(ctx, nil, runtimestorage.ListOptions{})
	if err != nil {
		klog.V(2).InfoS("s3-poll-failed", "err", err)
		return
	}
	list := result.(*aggexp.BucketList)

	next := make(map[string]*aggexp.Bucket, len(list.Items))
	for i := range list.Items {
		it := list.Items[i]
		next[it.Name] = it.DeepCopy()
	}

	b.mu.Lock()
	prev := b.seen
	var added, modified, deleted []*aggexp.Bucket
	for name, cur := range next {
		old, existed := prev[name]
		if !existed {
			added = append(added, cur)
		} else if !bucketEqual(old, cur) {
			// Preserve creation timestamp / UID from previous
			// observation; applyIdentity has already done this via
			// b.uids, but the status fields may differ.
			modified = append(modified, cur)
		}
	}
	for name, old := range prev {
		if _, still := next[name]; !still {
			deleted = append(deleted, old.DeepCopy())
		}
	}
	b.seen = next
	b.ready = true
	b.mu.Unlock()

	if b.publisher != nil {
		for _, o := range added {
			b.publisher.PublishAdded(o.DeepCopy())
		}
		for _, o := range modified {
			b.publisher.PublishModified(o.DeepCopy())
		}
		for _, o := range deleted {
			b.publisher.PublishDeleted(o.DeepCopy())
		}
	}

	klog.V(2).InfoS("s3-poll",
		"count", len(next), "added", len(added), "modified", len(modified),
		"deleted", len(deleted), "took", time.Since(t0))
}

// ---- helpers ----

func (b *Backend) getTags(ctx context.Context, name string) (map[string]string, error) {
	out, err := b.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(name)})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "NoSuchTagSet" {
			return map[string]string{}, nil
		}
		return nil, err
	}
	m := make(map[string]string, len(out.TagSet))
	for _, t := range out.TagSet {
		m[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	return m, nil
}

func (b *Backend) putTags(ctx context.Context, name string, tags map[string]string) error {
	if len(tags) == 0 {
		return nil // S3 rejects empty tag sets; skip.
	}
	tagSet := make([]s3types.Tag, 0, len(tags))
	for k, v := range tags {
		tagSet = append(tagSet, s3types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	_, err := b.client.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
		Bucket:  aws.String(name),
		Tagging: &s3types.Tagging{TagSet: tagSet},
	})
	return err
}

func (b *Backend) matchesPrefix(name string) bool {
	return b.prefix == "" || strings.HasPrefix(name, b.prefix)
}

// applyIdentity stamps UID and (if missing) CreationTimestamp on obj.
// It preserves the UID across reads so clients see stable identity
// within this AA's lifetime.
func (b *Backend) applyIdentity(obj *aggexp.Bucket) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if uid, ok := b.uids[obj.Name]; ok {
		obj.UID = uid
	} else {
		uid := types.UID(uuid.New().String())
		b.uids[obj.Name] = uid
		obj.UID = uid
	}
	if obj.CreationTimestamp.IsZero() {
		if obj.Status.CreationDate != nil {
			obj.CreationTimestamp = *obj.Status.CreationDate
		} else {
			obj.CreationTimestamp = metav1.NewTime(time.Now())
		}
	}
}

// preserveManagedMeta copies ObjectMeta fields that the library is
// responsible for (managedFields, labels, annotations, uid,
// creationTimestamp) from `src` (the object handed to us by the
// library, carrying those fields) onto `dst` (the fresh live read
// from AWS). Spec/Status come from dst because AWS is authoritative;
// metadata bookkeeping comes from src because AWS does not know
// about it.
func preserveManagedMeta(dst, src *aggexp.Bucket) {
	if src == nil {
		return
	}
	if src.UID != "" {
		dst.UID = src.UID
	}
	if !src.CreationTimestamp.IsZero() {
		dst.CreationTimestamp = src.CreationTimestamp
	}
	if src.ResourceVersion != "" {
		dst.ResourceVersion = src.ResourceVersion
	}
	if len(src.ManagedFields) > 0 {
		dst.ManagedFields = append([]metav1.ManagedFieldsEntry(nil), src.ManagedFields...)
	}
	if len(src.Labels) > 0 {
		if dst.Labels == nil {
			dst.Labels = map[string]string{}
		}
		for k, v := range src.Labels {
			dst.Labels[k] = v
		}
	}
	if len(src.Annotations) > 0 {
		if dst.Annotations == nil {
			dst.Annotations = map[string]string{}
		}
		for k, v := range src.Annotations {
			dst.Annotations[k] = v
		}
	}
}

func isNotFoundErr(err error) bool {
	var nsb *s3types.NoSuchBucket
	var nf *s3types.NotFound
	if errors.As(err, &nsb) || errors.As(err, &nf) {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchBucket", "NotFound":
			return true
		}
	}
	return false
}

func bucketEqual(a, b *aggexp.Bucket) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Spec.Region != b.Spec.Region {
		return false
	}
	if len(a.Spec.Tags) != len(b.Spec.Tags) {
		return false
	}
	for k, v := range a.Spec.Tags {
		if b.Spec.Tags[k] != v {
			return false
		}
	}
	return true
}

func translateTimestampSince(t metav1.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	d := time.Since(t.Time)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func logUser(verb string, u user.Info, fields ...interface{}) {
	if u == nil {
		klog.V(2).InfoS("s3-backend", append([]interface{}{"verb", verb}, fields...)...)
		return
	}
	kv := append([]interface{}{"verb", verb, "user", u.GetName(), "groups", u.GetGroups()}, fields...)
	klog.V(2).InfoS("s3-backend", kv...)
}

// Compile-time assertions.
var (
	_ runtimestorage.Backend         = (*Backend)(nil)
	_ runtimestorage.WritableBackend = (*Backend)(nil)
)
