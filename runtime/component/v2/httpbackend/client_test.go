package httpbackend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	componentv2pb "github.com/cheeseandcereal/aggexp/runtime/component/v2/proto"
)

// fakeServer provides the minimum endpoints the Client exercises.
func fakeServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/schema", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"group":"notes.aggexp.io","version":"v1","resource":"notes",
			"kind":"Note","singular":"note","namespaced":true,"writable":true,
			"supportsServerSideApply":true,"watchCapability":"push",
			"schema":{"type":"object"},
			"columns":[{"name":"Name","type":"string"}],
			"rowFields":[".metadata.name"]
		}`))
	})
	mux.HandleFunc("/objects/default/hello", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if r.Header.Get("X-Aggexp-User-Name") != "alice" {
				t.Errorf("user header not forwarded: %v", r.Header)
			}
			w.Write([]byte(`{"metadata":{"name":"hello","namespace":"default"},"spec":{"title":"H"}}`))
		case http.MethodPut:
			w.WriteHeader(http.StatusOK)
			body, _ := io.ReadAll(r.Body)
			w.Write(body)
		case http.MethodDelete:
			w.Write([]byte(`{"metadata":{"name":"hello"}}`))
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("/objects/default", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			body, _ := io.ReadAll(r.Body)
			w.Write(body)
			return
		}
		w.Write([]byte(`{"items":[{"metadata":{"name":"hello"}},{"metadata":{"name":"world"}}]}`))
	})
	mux.HandleFunc("/watch/default", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Errorf("Accept header wrong: %q", r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		f, _ := w.(http.Flusher)
		fmt.Fprintf(w, ": keepalive\n\n")
		if f != nil {
			f.Flush()
		}
		fmt.Fprintf(w, "data: {\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"hello\"}}}\n\n")
		if f != nil {
			f.Flush()
		}
		fmt.Fprintf(w, "data: {\"type\":\"MODIFIED\",\"object\":{\"metadata\":{\"name\":\"hello\",\"resourceVersion\":\"2\"}}}\n\n")
		if f != nil {
			f.Flush()
		}
	})
	mux.HandleFunc("/validate", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		// Deny on name=forbidden.
		if req["name"] == "forbidden" {
			w.Write([]byte(`{"allowed":false,"causes":[{"field":"metadata.name","message":"forbidden"}]}`))
			return
		}
		w.Write([]byte(`{"allowed":true}`))
	})
	mux.HandleFunc("/mutate", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"mutatedObject":{"metadata":{"name":"mutated"}}}`))
	})
	return httptest.NewServer(mux)
}

func TestClient_GetSchema(t *testing.T) {
	srv := fakeServer(t)
	defer srv.Close()
	c, err := New(srv.URL, 5*time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	resp, err := c.GetSchema(context.Background(), &componentv2pb.GetSchemaRequest{})
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}
	if resp.GetGroup() != "notes.aggexp.io" {
		t.Errorf("group=%s", resp.GetGroup())
	}
	if resp.GetWatchCapability() != "push" {
		t.Errorf("watchCapability=%s", resp.GetWatchCapability())
	}
	if len(resp.GetColumns()) != 1 {
		t.Errorf("columns: %v", resp.GetColumns())
	}
}

func TestClient_Get_ForwardsIdentity(t *testing.T) {
	srv := fakeServer(t)
	defer srv.Close()
	c, _ := New(srv.URL, 5*time.Second)
	resp, err := c.Get(context.Background(), &componentv2pb.GetRequest{
		User:      &componentv2pb.UserInfo{Name: "alice"},
		Namespace: "default", Name: "hello",
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !strings.Contains(string(resp.GetObjectJson()), `"name":"hello"`) {
		t.Errorf("body: %s", resp.GetObjectJson())
	}
}

func TestClient_List(t *testing.T) {
	srv := fakeServer(t)
	defer srv.Close()
	c, _ := New(srv.URL, 5*time.Second)
	resp, err := c.List(context.Background(), &componentv2pb.ListRequest{Namespace: "default"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resp.GetItemsJson()) != 2 {
		t.Errorf("items: %d", len(resp.GetItemsJson()))
	}
}

func TestClient_Watch_SSE(t *testing.T) {
	srv := fakeServer(t)
	defer srv.Close()
	c, _ := New(srv.URL, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := c.Watch(ctx, &componentv2pb.WatchRequest{Namespace: "default"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	ev1, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv 1: %v", err)
	}
	if ev1.GetType() != componentv2pb.EventType_EVENT_ADDED {
		t.Errorf("event 1 type: %v", ev1.GetType())
	}
	ev2, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv 2: %v", err)
	}
	if ev2.GetType() != componentv2pb.EventType_EVENT_MODIFIED {
		t.Errorf("event 2 type: %v", ev2.GetType())
	}
}

func TestClient_Validate_DenyWithCauses(t *testing.T) {
	srv := fakeServer(t)
	defer srv.Close()
	c, _ := New(srv.URL, 5*time.Second)
	resp, err := c.Validate(context.Background(), &componentv2pb.ValidateRequest{
		Namespace: "default", Name: "forbidden", Operation: "CREATE",
		ObjectJson: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if resp.GetAllowed() {
		t.Errorf("expected denied")
	}
	if len(resp.GetCauses()) != 1 || resp.GetCauses()[0].GetField() != "metadata.name" {
		t.Errorf("causes: %+v", resp.GetCauses())
	}
}

func TestHeaderEscape(t *testing.T) {
	cases := map[string]string{
		"authentication.kubernetes.io/credential-id": "authentication.kubernetes.io%2Fcredential-id",
		"plain":                                      "plain",
	}
	for in, want := range cases {
		if got := headerEscapeExtraKey(in); got != want {
			t.Errorf("headerEscapeExtraKey(%q)=%q, want %q", in, got, want)
		}
	}
}
