// Command mock-github is a stand-in for a slice of GitHub's REST
// API, used by experiment 0006. It serves:
//
//   GET /users/{owner}/repos    -> 3-5 canned repos for that owner
//   GET /repos/{owner}/{repo}   -> one canned repo, if known
//
// All requests must carry an Authorization: Bearer fake-token-*
// header. The service validates the token by calling the broker's
// /introspect endpoint. If the token is unknown, expired, or the
// token's scoped (user, owner, action) does not cover the request,
// mock-github returns 403 (consistent with how a real GitHub App
// installation token would behave outside its scope).
//
// There is no real GitHub call. The point of this service is to
// exercise the *plumbing*: AA -> broker -> mock-github -> AA, and
// to prove that the bearer token the AA presents is caller-scoped.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// -----------------------------------------------------------------------------
// canned repo data
// -----------------------------------------------------------------------------

// Repo mirrors the subset of the GitHub REST repository shape that
// the AA's github client consumes.
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

// canned returns a fresh slice of repos for owner. We hand out
// between 3 and 5 repos per owner; enough for a list to look
// real without needing pagination.
//
// Languages / star counts are made up; the AA doesn't care about
// the exact values, only that the fields exist.
func canned(owner string) []Repo {
	base := []struct {
		name string
		desc string
		lang string
		star int
	}{
		{"alpha", "the alpha repo", "Go", 12},
		{"beta", "the beta repo", "Python", 34},
		{"gamma", "the gamma repo", "Rust", 56},
		{"delta", "the delta repo", "Go", 78},
	}
	// Deterministic per-owner: shift count and star values so two
	// owners look different in a kubectl output.
	n := 3 + len(owner)%3 // 3, 4, or 5
	if n > len(base) {
		n = len(base)
	}
	out := make([]Repo, 0, n)
	for i := 0; i < n; i++ {
		r := Repo{
			ID:            int64(1000 + i*7 + int(owner[0])),
			Name:          base[i].name,
			FullName:      owner + "/" + base[i].name,
			Description:   base[i].desc + " (" + owner + ")",
			DefaultBranch: "main",
			Private:       false,
			Language:      base[i].lang,
			Stars:         base[i].star + len(owner),
			HTMLURL:       "https://example.invalid/" + owner + "/" + base[i].name,
		}
		r.Owner.Login = owner
		out = append(out, r)
	}
	return out
}

// -----------------------------------------------------------------------------
// broker introspection
// -----------------------------------------------------------------------------

type IntrospectRequest struct {
	Token string `json:"token"`
}

type IntrospectResponse struct {
	Valid   bool     `json:"valid"`
	User    string   `json:"user,omitempty"`
	Owner   string   `json:"owner,omitempty"`
	Actions []string `json:"actions,omitempty"`
	Reason  string   `json:"reason,omitempty"`
}

type broker struct {
	url    string
	client *http.Client
}

func (b *broker) introspect(token string) (*IntrospectResponse, error) {
	body, _ := json.Marshal(IntrospectRequest{Token: token})
	req, err := http.NewRequest(http.MethodPost, b.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b2, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("introspect status=%d body=%q", resp.StatusCode, string(b2))
	}
	var out IntrospectResponse
	if err := json.Unmarshal(b2, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// -----------------------------------------------------------------------------
// HTTP
// -----------------------------------------------------------------------------

// authorize validates the Authorization header against the broker
// and returns the introspection record, or an HTTP error.
func (b *broker) authorize(r *http.Request, wantOwner, wantAction string) (*IntrospectResponse, int, string) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return nil, http.StatusUnauthorized, "missing Bearer"
	}
	token := strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	if token == "" {
		return nil, http.StatusUnauthorized, "empty token"
	}
	if !strings.HasPrefix(token, "fake-token-") {
		return nil, http.StatusUnauthorized, "not a broker-issued token"
	}
	intro, err := b.introspect(token)
	if err != nil {
		return nil, http.StatusBadGateway, "introspect: " + err.Error()
	}
	if !intro.Valid {
		return nil, http.StatusUnauthorized, "invalid: " + intro.Reason
	}
	// Owner scope.
	if intro.Owner != "" && intro.Owner != wantOwner {
		return nil, http.StatusForbidden,
			fmt.Sprintf("token owner=%q does not cover %q", intro.Owner, wantOwner)
	}
	// Action scope.
	if !actionAllowed(intro.Actions, wantAction) {
		return nil, http.StatusForbidden,
			fmt.Sprintf("token actions=%v do not cover %q", intro.Actions, wantAction)
	}
	return intro, 0, ""
}

func actionAllowed(actions []string, want string) bool {
	if len(actions) == 0 {
		return false
	}
	for _, a := range actions {
		if a == "*" || a == want {
			return true
		}
		if strings.HasSuffix(a, "*") && strings.HasPrefix(want, a[:len(a)-1]) {
			return true
		}
	}
	return false
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	brokerURL := flag.String("broker-url", "http://broker.aggexp-system.svc/introspect",
		"URL of the broker's /introspect endpoint")
	flag.Parse()

	b := &broker{
		url:    *brokerURL,
		client: &http.Client{Timeout: 3 * time.Second},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})

	// GET /users/{owner}/repos -> []Repo
	mux.HandleFunc("/users/", func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/users/")
		parts := strings.Split(p, "/")
		if len(parts) != 2 || parts[1] != "repos" {
			http.NotFound(w, r)
			return
		}
		owner := parts[0]
		intro, code, msg := b.authorize(r, owner, "list")
		if code != 0 {
			log.Printf("GET %s -> %d (%s)", r.URL.Path, code, msg)
			writeErr(w, code, msg)
			return
		}
		log.Printf("GET %s user=%s owner=%s action=list -> 200", r.URL.Path, intro.User, intro.Owner)
		repos := canned(owner)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(repos)
	})

	// GET /repos/{owner}/{repo} -> Repo
	mux.HandleFunc("/repos/", func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/repos/")
		parts := strings.Split(p, "/")
		if len(parts) != 2 {
			http.NotFound(w, r)
			return
		}
		owner, name := parts[0], parts[1]
		intro, code, msg := b.authorize(r, owner, "get")
		if code != 0 {
			log.Printf("GET %s -> %d (%s)", r.URL.Path, code, msg)
			writeErr(w, code, msg)
			return
		}
		for _, repo := range canned(owner) {
			if repo.Name == name {
				log.Printf("GET %s user=%s owner=%s action=get -> 200", r.URL.Path, intro.User, intro.Owner)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(repo)
				return
			}
		}
		writeErr(w, http.StatusNotFound, "not found")
	})

	log.Printf("mock-github listening on %s broker=%s", *addr, *brokerURL)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": msg})
}
