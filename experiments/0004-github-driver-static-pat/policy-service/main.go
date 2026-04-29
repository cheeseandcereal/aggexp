// Command policy-service is a toy external authorization policy
// consulted by the aggexp-authz extension apiserver. It reads a
// JSON rules file at startup and makes allow/deny decisions over
// POST /authorize.
//
// Rule shape (rules.json):
//
//	{
//	  "rules": [
//	    { "user": "alice", "verb": "*", "name": "*", "allow": true,
//	      "reason": "alice is trusted" },
//	    { "user": "bob", "verb": "get", "allow": true },
//	    { "group": "readers", "verb": "get|list|watch", "allow": true,
//	      "reason": "readers can read" }
//	  ],
//	  "default": {"allow": false, "reason": "no rule matched"}
//	}
//
// Empty string / missing fields in a rule match anything. Verb and
// name support `*` as any-match and `a|b|c` as alternation.
//
// The service also hot-reloads the rules file on SIGHUP. This is a
// lab service; no auth, no TLS; runs in the same namespace as the AA.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Rule struct {
	User     string `json:"user,omitempty"`
	Group    string `json:"group,omitempty"`
	Verb     string `json:"verb,omitempty"`
	Resource string `json:"resource,omitempty"`
	Name     string `json:"name,omitempty"`
	Allow    bool   `json:"allow"`
	Reason   string `json:"reason,omitempty"`
}

type RuleFile struct {
	Rules   []Rule `json:"rules"`
	Default struct {
		Allow  bool   `json:"allow"`
		Reason string `json:"reason,omitempty"`
	} `json:"default"`
}

type Request struct {
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

type Response struct {
	Allow  bool   `json:"allow"`
	Reason string `json:"reason,omitempty"`
}

type Store struct {
	path string
	mu   sync.RWMutex
	file RuleFile
}

func (s *Store) load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	var rf RuleFile
	if err := json.Unmarshal(b, &rf); err != nil {
		return err
	}
	s.mu.Lock()
	s.file = rf
	s.mu.Unlock()
	log.Printf("loaded %d rules from %s (default: allow=%t)", len(rf.Rules), s.path, rf.Default.Allow)
	return nil
}

func (s *Store) evaluate(req Request) Response {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, r := range s.file.Rules {
		if !matchesRule(r, req) {
			continue
		}
		log.Printf("rule[%d] matched -> allow=%t user=%s verb=%s resource=%s name=%s",
			i, r.Allow, req.User, req.Verb, req.Resource, req.Name)
		return Response{Allow: r.Allow, Reason: r.Reason}
	}
	log.Printf("no rule matched -> default allow=%t user=%s verb=%s resource=%s name=%s",
		s.file.Default.Allow, req.User, req.Verb, req.Resource, req.Name)
	return Response{Allow: s.file.Default.Allow, Reason: s.file.Default.Reason}
}

func matchesRule(r Rule, req Request) bool {
	if r.User != "" && !matchField(r.User, req.User) {
		return false
	}
	if r.Group != "" {
		ok := false
		for _, g := range req.Groups {
			if matchField(r.Group, g) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if r.Verb != "" && !matchField(r.Verb, req.Verb) {
		return false
	}
	if r.Resource != "" && !matchField(r.Resource, req.Resource) {
		return false
	}
	if r.Name != "" && !matchField(r.Name, req.Name) {
		return false
	}
	return true
}

// matchField matches `*` (bare wildcard), `prefix*` (prefix glob),
// `a|b|c` (alternation where each branch is itself matched
// recursively), and literal (exact). Order matters: check `*` and
// alternation before literal.
func matchField(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	if strings.Contains(pattern, "|") {
		for _, p := range strings.Split(pattern, "|") {
			if matchField(p, value) {
				return true
			}
		}
		return false
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(value, pattern[:len(pattern)-1])
	}
	return pattern == value
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	rulesPath := flag.String("rules", "/etc/policy/rules.json", "path to JSON rules file")
	flag.Parse()

	store := &Store{path: *rulesPath}
	if err := store.load(); err != nil {
		log.Fatalf("load rules: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			if err := store.load(); err != nil {
				log.Printf("reload failed: %v", err)
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/authorize", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := store.evaluate(req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("policy-service listening on %s rules=%s", *addr, *rulesPath)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}
