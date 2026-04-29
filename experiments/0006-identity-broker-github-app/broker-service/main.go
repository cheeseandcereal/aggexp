// Command broker-service is the mock identity broker for experiment
// 0006. It receives identity-to-fake-token exchange requests from
// the aggregated apiserver and hands back short-lived "tokens" that
// the mock-github service validates via an introspection callback.
//
// The broker is the implementation of the "on behalf of the caller"
// pattern: the AA never sees a long-lived credential, and every
// outbound GitHub-ish call is scoped to a specific caller + owner +
// action for a few minutes.
//
// Wire shape (POST /exchange, JSON):
//
//	request:  {"user":"alice","groups":["system:authenticated"],
//	           "uid":"", "extra":{...},
//	           "owner":"kubernetes-sigs","repo":"","action":"list"}
//	response: {"token":"fake-token-alice-<rand>","expiresIn":300,
//	           "reason":"rule[0]"}
//
// On denial the broker returns 403 with
// {"reason":"<why>"} and no token.
//
// POST /introspect validates a previously-issued token. Used by the
// mock-github service to confirm the Bearer header before serving.
// Response: {"valid":true,"user":"alice","owner":"...",
//            "actions":["list","get"]}
//
// Rules are loaded at startup (and on SIGHUP) from a JSON file.
// Shape:
//
//	{
//	  "rules": [
//	    {"user":"kubernetes-admin","owners":"*","actions":"*",
//	     "reason":"admin: all GitHub access"},
//	    {"user":"alice","owners":"*","actions":"list|get",
//	     "reason":"alice: read-only"},
//	    {"user":"bob","owners":"bob-*","actions":"*",
//	     "reason":"bob: only own owners"},
//	    {"user":"mallory","deny":true,"reason":"mallory: no GitHub"}
//	  ],
//	  "default": {"deny": true, "reason": "no matching rule"}
//	}
//
// Matching is user-by-user exact; owners/actions use `*`, `prefix*`,
// and `a|b|c` alternation (same toy DSL as 0003). First matching
// rule wins.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

// -----------------------------------------------------------------------------
// rule file
// -----------------------------------------------------------------------------

type Rule struct {
	User    string `json:"user,omitempty"`
	Group   string `json:"group,omitempty"`
	Owners  string `json:"owners,omitempty"`
	Actions string `json:"actions,omitempty"`
	Deny    bool   `json:"deny,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

type Default struct {
	Deny   bool   `json:"deny"`
	Reason string `json:"reason,omitempty"`
}

type RuleFile struct {
	Rules   []Rule  `json:"rules"`
	Default Default `json:"default"`
}

// -----------------------------------------------------------------------------
// wire types
// -----------------------------------------------------------------------------

type ExchangeRequest struct {
	User   string              `json:"user"`
	Groups []string            `json:"groups,omitempty"`
	UID    string              `json:"uid,omitempty"`
	Extra  map[string][]string `json:"extra,omitempty"`
	Owner  string              `json:"owner"`
	Repo   string              `json:"repo,omitempty"`
	Action string              `json:"action"`
}

type ExchangeResponse struct {
	Token     string `json:"token,omitempty"`
	ExpiresIn int    `json:"expiresIn,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

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

// -----------------------------------------------------------------------------
// token store
// -----------------------------------------------------------------------------

// TokenRecord is what the broker remembers about a token it issued.
type TokenRecord struct {
	User    string
	Groups  []string
	Owner   string
	Actions []string
	Expires time.Time
}

type Store struct {
	rulesPath string

	mu     sync.RWMutex
	rules  RuleFile
	tokens map[string]*TokenRecord
}

func newStore(rulesPath string) *Store {
	return &Store{
		rulesPath: rulesPath,
		tokens:    make(map[string]*TokenRecord),
	}
}

func (s *Store) loadRules() error {
	b, err := os.ReadFile(s.rulesPath)
	if err != nil {
		return err
	}
	var rf RuleFile
	if err := json.Unmarshal(b, &rf); err != nil {
		return err
	}
	s.mu.Lock()
	s.rules = rf
	s.mu.Unlock()
	log.Printf("loaded %d rules from %s (default: deny=%t)", len(rf.Rules), s.rulesPath, rf.Default.Deny)
	return nil
}

// evaluate consults the rules and returns either a newly-issued
// token record (on allow) or a human reason string (on deny).
func (s *Store) evaluate(req ExchangeRequest) (*TokenRecord, string) {
	s.mu.RLock()
	rules := s.rules.Rules
	def := s.rules.Default
	s.mu.RUnlock()

	for i, r := range rules {
		if !ruleMatches(r, req) {
			continue
		}
		if r.Deny {
			log.Printf("rule[%d] DENY user=%s owner=%s action=%s reason=%q",
				i, req.User, req.Owner, req.Action, r.Reason)
			return nil, r.Reason
		}
		actions := expandActions(r.Actions)
		rec := &TokenRecord{
			User:    req.User,
			Groups:  req.Groups,
			Owner:   req.Owner,
			Actions: actions,
			Expires: time.Now().Add(300 * time.Second),
		}
		log.Printf("rule[%d] ALLOW user=%s owner=%s action=%s actions=%v reason=%q",
			i, req.User, req.Owner, req.Action, actions, r.Reason)
		return rec, r.Reason
	}
	log.Printf("no rule matched: default deny=%t user=%s owner=%s action=%s",
		def.Deny, req.User, req.Owner, req.Action)
	if def.Deny {
		return nil, def.Reason
	}
	return &TokenRecord{
		User: req.User, Groups: req.Groups, Owner: req.Owner,
		Actions: []string{"*"},
		Expires: time.Now().Add(300 * time.Second),
	}, def.Reason
}

// ruleMatches tests whether a single rule matches an exchange
// request. Owners uses pattern matching; the requested Action must
// be covered by the rule's Actions pattern. Deny rules only need
// the identity to match; they short-circuit with no action check so
// a deny rule like {"user":"mallory","deny":true} covers every
// attempted action.
func ruleMatches(r Rule, req ExchangeRequest) bool {
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
	if r.Deny {
		return true
	}
	if r.Owners != "" && !matchField(r.Owners, req.Owner) {
		return false
	}
	if r.Actions != "" && !matchField(r.Actions, req.Action) {
		return false
	}
	return true
}

// expandActions turns a rule's action pattern into the flat list of
// actions the issued token is allowed to perform. `*` becomes
// ["*"]; "a|b|c" becomes ["a","b","c"]; a literal becomes [literal];
// "prefix*" is preserved as a single pattern the introspector will
// re-match. The mock-github service uses this list to authorize.
func expandActions(actions string) []string {
	if actions == "" {
		return []string{"*"}
	}
	parts := strings.Split(actions, "|")
	return parts
}

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

// -----------------------------------------------------------------------------
// token mint + introspect
// -----------------------------------------------------------------------------

func (s *Store) mint(rec *TokenRecord) string {
	var buf [6]byte
	_, _ = rand.Read(buf[:])
	// Embed the user in the token for easy observability; the
	// token is not a secret in this lab, but its structure helps
	// log-reading.
	token := "fake-token-" + sanitize(rec.User) + "-" + hex.EncodeToString(buf[:])
	s.mu.Lock()
	s.tokens[token] = rec
	s.mu.Unlock()
	return token
}

func (s *Store) introspect(token string) *TokenRecord {
	s.mu.RLock()
	rec, ok := s.tokens[token]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	if time.Now().After(rec.Expires) {
		s.mu.Lock()
		delete(s.tokens, token)
		s.mu.Unlock()
		return nil
	}
	return rec
}

// sanitize keeps a subset of characters safe for log / token shape.
func sanitize(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out.WriteRune(r)
		default:
			out.WriteRune('_')
		}
	}
	return out.String()
}

// -----------------------------------------------------------------------------
// HTTP
// -----------------------------------------------------------------------------

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	rulesPath := flag.String("rules", "/etc/broker/rules.json", "path to JSON rules file")
	flag.Parse()

	store := newStore(*rulesPath)
	if err := store.loadRules(); err != nil {
		log.Fatalf("load rules: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			if err := store.loadRules(); err != nil {
				log.Printf("reload failed: %v", err)
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/exchange", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		var req ExchangeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("exchange user=%s groups=%v uid=%s extras=%v owner=%s repo=%s action=%s",
			req.User, req.Groups, req.UID, req.Extra, req.Owner, req.Repo, req.Action)
		rec, reason := store.evaluate(req)
		w.Header().Set("Content-Type", "application/json")
		if rec == nil {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(ExchangeResponse{Reason: reason})
			return
		}
		token := store.mint(rec)
		_ = json.NewEncoder(w).Encode(ExchangeResponse{
			Token:     token,
			ExpiresIn: 300,
			Reason:    reason,
		})
	})
	mux.HandleFunc("/introspect", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		var req IntrospectRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rec := store.introspect(req.Token)
		w.Header().Set("Content-Type", "application/json")
		if rec == nil {
			log.Printf("introspect token=%s... -> invalid", truncate(req.Token, 20))
			_ = json.NewEncoder(w).Encode(IntrospectResponse{Valid: false, Reason: "unknown or expired token"})
			return
		}
		log.Printf("introspect token=%s... -> valid user=%s owner=%s actions=%v",
			truncate(req.Token, 20), rec.User, rec.Owner, rec.Actions)
		_ = json.NewEncoder(w).Encode(IntrospectResponse{
			Valid:   true,
			User:    rec.User,
			Owner:   rec.Owner,
			Actions: rec.Actions,
		})
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

	log.Printf("broker listening on %s rules=%s", *addr, *rulesPath)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
