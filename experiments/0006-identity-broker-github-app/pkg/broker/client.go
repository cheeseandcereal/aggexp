// Package broker is the aggregated apiserver's client for the mock
// identity broker used in experiment 0006. The broker exchanges a
// Kubernetes-native user.Info for a short-lived "GitHub" token
// scoped to (owner, action).
//
// The real-world analog is a GitHub App-backed broker that mints
// installation tokens. For this experiment, the "token" is a fake
// opaque string that only the mock-github service knows how to
// validate (by calling the broker's /introspect endpoint). The
// plumbing is what matters: the AA never holds a long-lived
// credential, and every downstream call carries a token bound to a
// specific caller.
package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/klog/v2"
)

// ExchangeRequest is the JSON body the AA sends on /exchange.
type ExchangeRequest struct {
	User   string              `json:"user"`
	Groups []string            `json:"groups,omitempty"`
	UID    string              `json:"uid,omitempty"`
	Extra  map[string][]string `json:"extra,omitempty"`
	Owner  string              `json:"owner"`
	Repo   string              `json:"repo,omitempty"`
	Action string              `json:"action"`
}

// ExchangeResponse is the JSON body the broker returns on /exchange.
type ExchangeResponse struct {
	Token     string `json:"token,omitempty"`
	ExpiresIn int    `json:"expiresIn,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// ErrDenied signals the broker refused to issue a token; the AA
// should treat this as "no GitHub access for this caller" rather
// than as an error condition.
type ErrDenied struct {
	Reason string
}

func (e *ErrDenied) Error() string {
	if e.Reason == "" {
		return "broker denied exchange"
	}
	return "broker denied exchange: " + e.Reason
}

// Client is a tiny HTTP client for the broker.
type Client struct {
	url    string
	client *http.Client
}

// New returns a Client ready to call the broker at url.
func New(url string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Client{
		url:    url,
		client: &http.Client{Timeout: timeout},
	}
}

// FetchToken exchanges the caller's identity for a short-lived
// scoped token. The (owner, repo, action) tuple describes the
// intended downstream operation.
//
// Returns *ErrDenied on 403 (policy said no); other errors indicate
// transport / broker failures. The AA surfaces denials as empty
// results (fail closed, no token -> no call), and surfaces
// transport errors as 500s.
func (c *Client) FetchToken(ctx context.Context, u user.Info, owner, repo, action string) (string, error) {
	if u == nil {
		return "", fmt.Errorf("broker: missing user.Info")
	}
	req := ExchangeRequest{
		User:   u.GetName(),
		Groups: u.GetGroups(),
		UID:    u.GetUID(),
		Extra:  cloneExtra(u.GetExtra()),
		Owner:  owner,
		Repo:   repo,
		Action: action,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("broker: marshal: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("broker: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	t0 := time.Now()
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("broker: call: %w", err)
	}
	defer resp.Body.Close()

	var decoded ExchangeResponse
	if derr := json.NewDecoder(resp.Body).Decode(&decoded); derr != nil && resp.StatusCode < 400 {
		return "", fmt.Errorf("broker: decode: %w", derr)
	}

	klog.V(2).InfoS("broker-exchange",
		"user", req.User, "groups", req.Groups, "owner", owner, "action", action,
		"status", resp.StatusCode, "reason", decoded.Reason, "took", time.Since(t0))

	if resp.StatusCode == http.StatusForbidden {
		return "", &ErrDenied{Reason: decoded.Reason}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("broker: status %d reason=%q", resp.StatusCode, decoded.Reason)
	}
	if decoded.Token == "" {
		return "", fmt.Errorf("broker: empty token in 2xx response")
	}
	return decoded.Token, nil
}

func cloneExtra(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for k, v := range in {
		out[k] = append([]string(nil), v...)
	}
	return out
}
