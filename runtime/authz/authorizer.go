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
)

// PolicyRequest is the JSON body sent to the policy service.
// Wire-compatible with experiment 0003's payload.
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

// DecisionLog is an optional callback invoked after every
// authorization decision. Consumers use it for structured logging
// or metrics. Errors (transport / decode failures) arrive with a
// non-nil err and decision == NoOpinion.
type DecisionLog func(req PolicyRequest, decision authorizer.Decision, reason string, err error)

// Options configures an external-policy Authorizer.
type Options struct {
	// URL is the fully-qualified endpoint the authorizer POSTs
	// PolicyRequest JSON to. Required.
	URL string
	// Timeout is the per-request HTTP timeout. Defaults to 2s if
	// zero.
	Timeout time.Duration
	// Group is the API group this authorizer opines on. Empty
	// means "opine on all resource requests."
	Group string
	// Log is an optional structured-log / metrics callback.
	Log DecisionLog
	// Transport is an optional override for the HTTP transport.
	// Exposed for testing (httptest round trippers etc.).
	Transport http.RoundTripper
}

// Authorizer calls an external HTTP policy service to make
// per-request authorization decisions for a specific API group.
type Authorizer struct {
	url    string
	group  string
	client *http.Client
	log    DecisionLog
}

// New returns an Authorizer suitable for chaining with
// union.New(ext, existing). On transport errors it returns NoOpinion
// so the chain's remaining authorizers still get a say; consumers
// that want fail-closed behavior should wrap this authorizer.
func New(opts Options) *Authorizer {
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 2 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	if opts.Transport != nil {
		client.Transport = opts.Transport
	}
	return &Authorizer{
		url:    opts.URL,
		group:  opts.Group,
		client: client,
		log:    opts.Log,
	}
}

// Authorize implements authorizer.Authorizer.
func (a *Authorizer) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	if !attrs.IsResourceRequest() {
		return authorizer.DecisionNoOpinion, "", nil
	}
	if a.group != "" && attrs.GetAPIGroup() != a.group {
		return authorizer.DecisionNoOpinion, "", nil
	}
	u := attrs.GetUser()
	if u == nil {
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
	if a.log != nil {
		a.log(req, decision, reason, err)
	}
	if err != nil {
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

// Compile-time assertion.
var _ authorizer.Authorizer = (*Authorizer)(nil)
