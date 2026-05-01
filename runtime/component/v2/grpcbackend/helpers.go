package grpcbackend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	"github.com/cheeseandcereal/aggexp/runtime/component/v2/admission"
	"github.com/cheeseandcereal/aggexp/runtime/component/v2/metadatastore"
	componentv2pb "github.com/cheeseandcereal/aggexp/runtime/component/v2/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/v2/scheme"
)

// businessJSON strips everything but apiVersion, kind, metadata.name,
// metadata.namespace, spec, and status from obj — the backend sees
// only this narrow payload (per the FINDINGS/0024 thesis).
func businessJSON(raw runtime.Object) ([]byte, error) {
	out, err := encodeObject(raw)
	if err != nil {
		return nil, err
	}
	return businessJSONFromBytes(out)
}

func businessJSONFromBytes(raw []byte) ([]byte, error) {
	var full map[string]any
	if err := json.Unmarshal(raw, &full); err != nil {
		return nil, fmt.Errorf("unmarshal obj: %w", err)
	}
	slim := map[string]any{}
	if v, ok := full["apiVersion"].(string); ok {
		slim["apiVersion"] = v
	}
	if v, ok := full["kind"].(string); ok {
		slim["kind"] = v
	}
	if md, ok := full["metadata"].(map[string]any); ok {
		slimMD := map[string]any{}
		if v, ok := md["name"]; ok {
			slimMD["name"] = v
		}
		if v, ok := md["namespace"]; ok && v != "" {
			slimMD["namespace"] = v
		}
		slim["metadata"] = slimMD
	}
	if v, ok := full["spec"]; ok {
		slim["spec"] = v
	}
	if v, ok := full["status"]; ok {
		slim["status"] = v
	}
	return json.Marshal(slim)
}

// encodeObject renders obj as JSON, handling the two supported
// runtime.Object shapes (Object and unstructured.Unstructured).
func encodeObject(obj runtime.Object) ([]byte, error) {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.MarshalJSON()
	}
	if o, ok := obj.(*componentscheme.Object); ok {
		return o.MarshalJSON()
	}
	return json.Marshal(obj)
}

// recordFromLibraryObject pulls ObjectMeta out of obj into a
// metastore Record skeleton.
func recordFromLibraryObject(obj runtime.Object, ref metadatastore.ResourceRef) *metadatastore.Record {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return &metadatastore.Record{Ref: ref}
	}
	rec := &metadatastore.Record{
		Ref:               ref,
		UID:               string(acc.GetUID()),
		CreationTimestamp: acc.GetCreationTimestamp(),
		Labels:            mapCopy(acc.GetLabels()),
		Annotations:       mapCopy(acc.GetAnnotations()),
		Finalizers:        append([]string(nil), acc.GetFinalizers()...),
	}
	if dt := acc.GetDeletionTimestamp(); dt != nil && !dt.IsZero() {
		rec.DeletionTimestamp = dt
	}
	if mf := acc.GetManagedFields(); len(mf) > 0 {
		if raw, err := json.Marshal(mf); err == nil {
			rec.ManagedFields = raw
		}
	}
	if or := acc.GetOwnerReferences(); len(or) > 0 {
		if raw, err := json.Marshal(or); err == nil {
			rec.OwnerReferences = raw
		}
	}
	return rec
}

func userFromCtx(ctx context.Context) *componentv2pb.UserInfo {
	v, ok := genericapirequest.UserFrom(ctx)
	if !ok || v == nil {
		return nil
	}
	return userToProto(v)
}

func userToProto(u user.Info) *componentv2pb.UserInfo {
	out := &componentv2pb.UserInfo{
		Name:   u.GetName(),
		Uid:    u.GetUID(),
		Groups: u.GetGroups(),
		Extra:  map[string]*componentv2pb.StringList{},
	}
	for k, v := range u.GetExtra() {
		out.Extra[k] = &componentv2pb.StringList{Values: v}
	}
	return out
}

func selectorString(opts *metainternalversion.ListOptions) string {
	if opts == nil || opts.LabelSelector == nil || opts.LabelSelector.Empty() {
		return ""
	}
	return opts.LabelSelector.String()
}

func selectorFromOpts(opts *metainternalversion.ListOptions) labels.Selector {
	if opts == nil || opts.LabelSelector == nil {
		return labels.Everything()
	}
	return opts.LabelSelector
}

func updateFM(o *metav1.UpdateOptions) string {
	if o == nil {
		return ""
	}
	return o.FieldManager
}

func createFM(o *metav1.CreateOptions) string {
	if o == nil {
		return ""
	}
	return o.FieldManager
}

func mapCopy(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func nameNamespaceFromJSON(raw []byte) (string, string) {
	var m struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", ""
	}
	return m.Metadata.Name, m.Metadata.Namespace
}

func nameOfRaw(raw []byte) string {
	n, _ := nameNamespaceFromJSON(raw)
	return n
}

// LookupField is a tiny dotted-path lookup (".", split on '.').
// Exported for table-row generation in consumers.
func LookupField(obj map[string]any, path string) any {
	if path == "" {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(path, "."), ".")
	var cur any = obj
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	if cur == nil {
		return ""
	}
	return cur
}

func ageOf(ts string) string {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return "<unknown>"
	}
	d := time.Since(t).Round(time.Second)
	return durationShort(d)
}

func durationShort(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// extractListItems returns a slice of runtime.Objects from either
// an ObjectList or UnstructuredList.
func extractListItems(list runtime.Object) []runtime.Object {
	switch l := list.(type) {
	case *componentscheme.ObjectList:
		out := make([]runtime.Object, 0, len(l.Items))
		for i := range l.Items {
			out = append(out, &l.Items[i])
		}
		return out
	case *unstructured.UnstructuredList:
		out := make([]runtime.Object, 0, len(l.Items))
		for i := range l.Items {
			out = append(out, &l.Items[i])
		}
		return out
	}
	return nil
}

// quickHash produces a content hash for the pollCache diff. The
// poll loop uses it to decide ADD vs MODIFIED; sha256 is overkill
// but cheap at this scale.
func quickHash(raw []byte) string {
	h := sha256.New()
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil))
}

// failuresToInvalid renders admission failures as HTTP 422 w/
// multi-cause `field.ErrorList`. Matches the wire shape
// kube-apiserver's built-in validation emits.
func failuresToInvalid(gr schema.GroupResource, name string, fs []admission.Failure) error {
	var list field.ErrorList
	for _, f := range fs {
		list = append(list, field.Invalid(field.NewPath(f.FieldPath), name, f.Message))
	}
	gk := schema.GroupKind{Group: gr.Group, Kind: gr.Resource}
	return apierrors.NewInvalid(gk, name, list)
}

// protoCausesToInvalid translates backend-RPC ValidateResponse
// causes into an HTTP 422 with the same wire shape.
func protoCausesToInvalid(gr schema.GroupResource, name string, resp *componentv2pb.ValidateResponse) error {
	var list field.ErrorList
	for _, c := range resp.GetCauses() {
		list = append(list, field.Invalid(field.NewPath(c.GetField()), name, c.GetMessage()))
	}
	if len(list) == 0 {
		msg := resp.GetReason()
		if msg == "" {
			msg = "backend admission denied"
		}
		return apierrors.NewBadRequest(msg)
	}
	gk := schema.GroupKind{Group: gr.Group, Kind: gr.Resource}
	return apierrors.NewInvalid(gk, name, list)
}

// errInterfaceAssertion helper to keep imports used even when
// select branches trim references. Deliberately a no-op.
var _ = errors.New
