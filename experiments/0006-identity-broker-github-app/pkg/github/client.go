// Package github is a minimal stdlib HTTP client for the bits of
// the GitHub REST API we need: listing a user/org's repositories
// and fetching a single repository by (owner, name).
//
// Experiment 0006 change from 0004: the client no longer carries a
// static token. Instead it delegates token acquisition to a
// TokenProvider — the identity broker. Every GitHub call minted
// from this client pays one broker round trip in exchange for a
// token scoped to the *current* caller + action.
package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"k8s.io/apiserver/pkg/authentication/user"
)

// Repo is the GitHub-side shape of a repository. We only map fields
// we surface.
type Repo struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	FullName      string `json:"full_name"`
	Description   string `json:"description"`
	DefaultBranch string `json:"default_branch"`
	Private       bool   `json:"private"`
	Language      string `json:"language"`
	Stars         int    `json:"stargazers_count"`
	HTMLURL       string `json:"html_url"`
	Owner         struct {
		Login string `json:"login"`
	} `json:"owner"`
}

// TokenProvider returns a short-lived bearer token scoped to the
// caller's identity and the intended action against owner/repo.
// Implementations: broker.Client (real plumbing in 0006), or a
// static-string shim for tests.
//
// If the provider returns a broker-denied error (identity has no
// GitHub access for this action), the github client's callers are
// expected to surface that as an empty result rather than a hard
// error — this is the "fail closed, quiet denial" path.
type TokenProvider interface {
	FetchToken(ctx context.Context, u user.Info, owner, repo, action string) (string, error)
}

// Client is a GitHub REST client that asks a TokenProvider for a
// bearer token on every call.
type Client struct {
	base     string
	provider TokenProvider
	client   *http.Client
}

// New returns a client configured to call the GitHub-shaped REST
// API at base. If base is empty, defaults to https://api.github.com.
// provider is required; without it the client cannot authorize any
// call.
func New(base string, provider TokenProvider) *Client {
	if base == "" {
		base = "https://api.github.com"
	}
	return &Client{
		base:     base,
		provider: provider,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// ListRepos returns up to perPage * maxPages repos for the given
// user or organization, on behalf of u.
func (c *Client) ListRepos(ctx context.Context, u user.Info, owner string, perPage, maxPages int) ([]Repo, error) {
	if perPage <= 0 {
		perPage = 30
	}
	if maxPages <= 0 {
		maxPages = 4
	}
	path := "/users/" + url.PathEscape(owner) + "/repos"
	out := make([]Repo, 0, perPage)

	for page := 1; page <= maxPages; page++ {
		q := url.Values{}
		q.Set("per_page", strconv.Itoa(perPage))
		q.Set("page", strconv.Itoa(page))
		q.Set("sort", "full_name")

		var pageRepos []Repo
		err := c.getJSON(ctx, u, owner, "", "list", path+"?"+q.Encode(), &pageRepos)
		if err != nil {
			return nil, err
		}
		out = append(out, pageRepos...)
		if len(pageRepos) < perPage {
			break
		}
	}
	return out, nil
}

// GetRepo fetches a single repository by (owner, name) on behalf of u.
func (c *Client) GetRepo(ctx context.Context, u user.Info, owner, name string) (*Repo, error) {
	path := "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(name)
	var r Repo
	if err := c.getJSON(ctx, u, owner, name, "get", path, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// ErrNotFound is returned when GitHub returns 404.
var ErrNotFound = fmt.Errorf("github: not found")

// getJSON fetches a caller-scoped token via the TokenProvider, then
// issues the HTTP request to the GitHub API with that token as the
// Bearer credential. The provider's denial is surfaced to the
// caller via provider-error types; callers decide whether to treat
// denial as empty-result vs. error.
func (c *Client) getJSON(ctx context.Context, u user.Info, owner, repo, action, path string, v interface{}) error {
	token, err := c.provider.FetchToken(ctx, u, owner, repo, action)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "aggexp/0.0 (experiment-0006)")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("github request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("github status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}
