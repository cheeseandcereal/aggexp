// Package authz wires an external-HTTP-service-driven
// authorizer.Authorizer into the aggregated apiserver.
//
// For requests outside the aggexp.io API group (e.g. /livez, /readyz,
// /metrics, non-resource paths) this authorizer returns NoOpinion so
// the union-chained library authorizers (privileged-groups, path
// always-allow, delegated SAR) continue to handle them.
//
// For aggexp.io resource requests, the authorizer POSTs a JSON
// payload to the configured external policy service and translates
// the response into Allow / Deny. On transport / decode errors it
// logs at V(2) and returns NoOpinion -- i.e., it declines to opine
// so the request continues down the union chain. Other experiments
// can probe the "fail closed vs fail open" semantics; for this
// experiment the safer default is "declining an opinion is not a
// silent allow because the HTTP filter upgrades NoOpinion to 403
// when no upstream authorizer said Allow".
package authz

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/klog/v2"
)

// PolicyRequest is the JSON body sent to the policy service.
type PolicyRequest struct {
	User        string              `json:"user"`
	Groups      []string            `json:"groups,omitempty"`
	UID         string              `json:"uid,omitempty"`
	Extra       map[string][]string `json:"extra,omitempty"`
	Verb        string              `json:"verb"`
	APIGroup    string              `json:"apiGroup"`
	APIVersion  string              `json:"apiVersion"`
	Resource    string              `json:"resource"`
	Subresource string              `json:"subresource,omitempty"`
	Namespace   string              `json:"namespace,omitempty"`
	Name        string              `json:"name,omitempty"`
}

// PolicyResponse is the JSON body the policy service returns.
type PolicyResponse struct {
	Allow  bool   `json:"allow"`
	Reason string `json:"reason,omitempty"`
}

// Options configures an ExternalPolicyAuthorizer. URL is required.
type Options struct {
	URL     string
	Timeout time.Duration
	Group   string // API group to gate; anything else returns NoOpinion.
}

// Authorizer calls an external HTTP policy service to make per-request
// authorization decisions for a specific API group.
type Authorizer struct {
	url    string
	group  string
	client *http.Client
}

// New returns an Authorizer ready to be union-chained after the
// library's default authorizers.
func New(opts Options) *Authorizer {
	if opts.Timeout == 0 {
		opts.Timeout = 2 * time.Second // arbitrary; loud in lab if wrong
	}
	return &Authorizer{
		url:   opts.URL,
		group: opts.Group,
		client: &http.Client{
			Timeout: opts.Timeout,
		},
	}
}

// Authorize implements authorizer.Authorizer.
func (a *Authorizer) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	// Scope: only opine on aggexp.io (or configured group) resource requests.
	if !attrs.IsResourceRequest() {
		return authorizer.DecisionNoOpinion, "", nil
	}
	if a.group != "" && attrs.GetAPIGroup() != a.group {
		return authorizer.DecisionNoOpinion, "", nil
	}

	u := attrs.GetUser()
	if u == nil {
		// Shouldn't happen under the aggregation layer; logged and pass.
		return authorizer.DecisionNoOpinion, "missing user", nil
	}

	req := PolicyRequest{
		User:        u.GetName(),
		Groups:      u.GetGroups(),
		UID:         u.GetUID(),
		Extra:       flattenExtras(u),
		Verb:        attrs.GetVerb(),
		APIGroup:    attrs.GetAPIGroup(),
		APIVersion:  attrs.GetAPIVersion(),
		Resource:    attrs.GetResource(),
		Subresource: attrs.GetSubresource(),
		Namespace:   attrs.GetNamespace(),
		Name:        attrs.GetName(),
	}

	decision, reason, err := a.ask(ctx, req)
	klog.V(2).InfoS("ext-authz",
		"user", req.User, "groups", req.Groups,
		"verb", req.Verb, "resource", req.Resource, "name", req.Name,
		"decision", decisionLabel(decision), "reason", reason, "err", err,
	)
	if err != nil {
		// Surfacing transport errors up to the HTTP filter via non-nil
		// err yields 500 (with Deny/NoOpinion). That's noisy for what
		// is likely an intermittent backend blip. Prefer NoOpinion +
		// log so the chain's SAR fallback can still decide -- and
		// when the chain bottoms out, 403 Forbidden is fine.
		return authorizer.DecisionNoOpinion, "policy service unavailable", nil
	}
	return decision, reason, nil
}

func (a *Authorizer) ask(ctx context.Context, req PolicyRequest) (authorizer.Decision, string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return authorizer.DecisionNoOpinion, "", fmt.Errorf("marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, bytes.NewReader(body))
	if err != nil {
		return authorizer.DecisionNoOpinion, "", fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return authorizer.DecisionNoOpinion, "", fmt.Errorf("policy service call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return authorizer.DecisionNoOpinion, "", fmt.Errorf("policy service status %d", resp.StatusCode)
	}

	var decoded PolicyResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return authorizer.DecisionNoOpinion, "", fmt.Errorf("decode response: %w", err)
	}
	if decoded.Allow {
		return authorizer.DecisionAllow, decoded.Reason, nil
	}
	return authorizer.DecisionDeny, decoded.Reason, nil
}

func flattenExtras(u user.Info) map[string][]string {
	m := u.GetExtra()
	if len(m) == 0 {
		return nil
	}
	out := make(map[string][]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func decisionLabel(d authorizer.Decision) string {
	switch d {
	case authorizer.DecisionAllow:
		return "allow"
	case authorizer.DecisionDeny:
		return "deny"
	default:
		return "noopinion"
	}
}

// Compile-time assertion.
var _ authorizer.Authorizer = (*Authorizer)(nil)
