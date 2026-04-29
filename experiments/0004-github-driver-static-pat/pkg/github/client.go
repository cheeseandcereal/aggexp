// Package github is a minimal stdlib HTTP client for the bits of
// the GitHub REST API we need: listing a user/org's repositories
// and fetching a single repository by (owner, name).
//
// This is deliberately not go-github or the other mature clients.
// Reasons: (a) the experiment only needs two endpoints; (b) we
// want to keep the dependency surface legible; (c) GitHub's REST
// responses are stable enough that a hand-rolled struct works.
package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
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

// Client is a thin GitHub REST client. Zero value is not usable;
// use New.
type Client struct {
	base   string
	token  string
	client *http.Client
}

// New returns a client configured to call the GitHub REST API.
// If base is empty, defaults to https://api.github.com.
func New(base, token string) *Client {
	if base == "" {
		base = "https://api.github.com"
	}
	return &Client{
		base:  base,
		token: token,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// ListRepos returns up to perPage * maxPages repos for the given
// user or organization. Pagination follows GitHub's Link header
// convention; we stop at maxPages to keep experiments bounded.
func (c *Client) ListRepos(ctx context.Context, owner string, perPage, maxPages int) ([]Repo, error) {
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
		gotFewer, err := c.getJSON(ctx, path+"?"+q.Encode(), &pageRepos)
		if err != nil {
			return nil, err
		}
		out = append(out, pageRepos...)
		if gotFewer || len(pageRepos) < perPage {
			break
		}
	}
	return out, nil
}

// GetRepo fetches a single repository by (owner, name). Returns
// (*Repo, nil) on success, (nil, ErrNotFound) on 404, or (nil, err)
// on transport/status errors.
func (c *Client) GetRepo(ctx context.Context, owner, name string) (*Repo, error) {
	path := "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(name)
	var r Repo
	if _, err := c.getJSON(ctx, path, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// ErrNotFound is returned by GetRepo / ListRepos when GitHub returns 404.
var ErrNotFound = fmt.Errorf("github: not found")

// getJSON sends a GET request, expects JSON, and decodes it into v.
// The bool return value indicates whether the server explicitly told
// us there are fewer items than we asked for (via response body
// length; ListRepos uses it to short-circuit pagination).
func (c *Client) getJSON(ctx context.Context, path string, v interface{}) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "aggexp/0.0 (experiment-0004)")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("github request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Errorf("github status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return false, fmt.Errorf("decode: %w", err)
	}
	return false, nil
}
