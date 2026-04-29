package authz_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"

	"github.com/cheeseandcereal/aggexp/runtime/authz"
)

type fakeAttrs struct {
	user        user.Info
	verb        string
	apiGroup    string
	apiVersion  string
	resource    string
	subresource string
	namespace   string
	name        string
	isResource  bool
	path        string
}

func newAttrs(f fakeAttrs) authorizer.Attributes {
	return authorizer.AttributesRecord{
		User:            f.user,
		Verb:            f.verb,
		Namespace:       f.namespace,
		APIGroup:        f.apiGroup,
		APIVersion:      f.apiVersion,
		Resource:        f.resource,
		Subresource:     f.subresource,
		Name:            f.name,
		ResourceRequest: f.isResource,
		Path:            f.path,
	}
}

func TestNoOpinionOutsideGroup(t *testing.T) {
	a := authz.New(authz.Options{URL: "http://unused", Group: "aggexp.io"})
	attrs := newAttrs(fakeAttrs{
		user: &user.DefaultInfo{Name: "x"}, verb: "get",
		apiGroup: "other.io", resource: "foos", isResource: true,
	})
	d, _, err := a.Authorize(context.Background(), attrs)
	if err != nil {
		t.Fatal(err)
	}
	if d != authorizer.DecisionNoOpinion {
		t.Fatalf("expected NoOpinion outside scope, got %v", d)
	}
}

func TestNoOpinionForNonResource(t *testing.T) {
	a := authz.New(authz.Options{URL: "http://unused", Group: "aggexp.io"})
	attrs := newAttrs(fakeAttrs{
		user: &user.DefaultInfo{Name: "x"}, verb: "get",
		apiGroup: "aggexp.io", path: "/healthz", isResource: false,
	})
	d, _, err := a.Authorize(context.Background(), attrs)
	if err != nil {
		t.Fatal(err)
	}
	if d != authorizer.DecisionNoOpinion {
		t.Fatalf("expected NoOpinion on non-resource, got %v", d)
	}
}

func TestAllowFromPolicyService(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req authz.PolicyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("bad payload: %v", err)
		}
		if req.User != "alice" {
			t.Fatalf("expected user=alice, got %s", req.User)
		}
		_ = json.NewEncoder(w).Encode(authz.PolicyResponse{Allow: true, Reason: "ok"})
	}))
	defer srv.Close()

	var logged struct {
		decision authorizer.Decision
		reason   string
		called   bool
	}
	a := authz.New(authz.Options{
		URL:   srv.URL,
		Group: "aggexp.io",
		Log: func(_ authz.PolicyRequest, d authorizer.Decision, reason string, _ error) {
			logged.decision = d
			logged.reason = reason
			logged.called = true
		},
	})
	attrs := newAttrs(fakeAttrs{
		user: &user.DefaultInfo{Name: "alice"}, verb: "get",
		apiGroup: "aggexp.io", apiVersion: "v1", resource: "things", isResource: true,
	})
	d, reason, err := a.Authorize(context.Background(), attrs)
	if err != nil {
		t.Fatal(err)
	}
	if d != authorizer.DecisionAllow {
		t.Fatalf("expected Allow, got %v (%s)", d, reason)
	}
	if !logged.called || logged.decision != authorizer.DecisionAllow || logged.reason != "ok" {
		t.Fatalf("decision log callback not invoked correctly: %+v", logged)
	}
}

func TestDenyFromPolicyService(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(authz.PolicyResponse{Allow: false, Reason: "policy says no"})
	}))
	defer srv.Close()
	a := authz.New(authz.Options{URL: srv.URL, Group: "aggexp.io"})
	attrs := newAttrs(fakeAttrs{
		user: &user.DefaultInfo{Name: "mallory"}, verb: "create",
		apiGroup: "aggexp.io", resource: "things", isResource: true,
	})
	d, reason, err := a.Authorize(context.Background(), attrs)
	if err != nil {
		t.Fatal(err)
	}
	if d != authorizer.DecisionDeny {
		t.Fatalf("expected Deny, got %v", d)
	}
	if reason != "policy says no" {
		t.Fatalf("expected reason verbatim, got %q", reason)
	}
}

func TestTransportErrorFallsThroughToNoOpinion(t *testing.T) {
	a := authz.New(authz.Options{
		URL:     "http://127.0.0.1:1", // connection refused
		Group:   "aggexp.io",
		Timeout: 200 * time.Millisecond,
	})
	attrs := newAttrs(fakeAttrs{
		user: &user.DefaultInfo{Name: "alice"}, verb: "get",
		apiGroup: "aggexp.io", resource: "things", isResource: true,
	})
	d, reason, err := a.Authorize(context.Background(), attrs)
	if err != nil {
		t.Fatalf("expected err=nil (fail-open-to-NoOpinion), got %v", err)
	}
	if d != authorizer.DecisionNoOpinion {
		t.Fatalf("expected NoOpinion on transport error, got %v", d)
	}
	if reason != "policy service unavailable" {
		t.Fatalf("expected canned reason, got %q", reason)
	}
}
