package componentv2pb

import "testing"

// TestGeneratedBindings is a smoke test that the generated .pb.go
// surface compiles and exposes the expected service + messages.
// Real over-the-wire tests live in grpcbackend and httpbackend.
func TestGeneratedBindings(t *testing.T) {
	req := &GetSchemaRequest{}
	_ = req
	// Build a response with every field to make sure the generated
	// struct has them.
	resp := &GetSchemaResponse{
		Group: "g", Version: "v", Resource: "r", Kind: "K",
		Singular: "k", Namespaced: true, Writable: true,
		Schema: []byte("{}"), SchemaIsOpenapi: true,
		SupportsServerSideApply: true,
		SupportsValidation:      true,
		SupportsMutation:        true,
		WatchCapability:         "push",
	}
	if resp.GetWatchCapability() != "push" {
		t.Errorf("WatchCapability not surfaced")
	}
	if !resp.GetSchemaIsOpenapi() {
		t.Errorf("SchemaIsOpenapi not surfaced")
	}
	// Watch event with BOOKMARK type.
	ev := &WatchEvent{Type: EventType_EVENT_BOOKMARK, ObjectJson: []byte("{}")}
	if ev.GetType() != EventType_EVENT_BOOKMARK {
		t.Errorf("EVENT_BOOKMARK round-trip broke")
	}
	// Validate response with multi-cause.
	vr := &ValidateResponse{Allowed: false, Causes: []*AdmissionCause{
		{Field: "spec.title", Message: "required"},
	}}
	if len(vr.GetCauses()) != 1 || vr.GetCauses()[0].GetField() != "spec.title" {
		t.Errorf("AdmissionCause round-trip: %v", vr)
	}
}
