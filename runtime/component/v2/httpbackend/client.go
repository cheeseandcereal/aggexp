// Package httpbackend implements the v2 Backend gRPC client
// interface over HTTP/JSON with SSE for Watch. From the
// middleware's perspective the transport is invisible: the same
// REST adapter works against either httpbackend.Client or
// grpcbackend.Client because both satisfy componentv2pb.BackendClient.
//
// Wire shape (the backend binds these routes):
//
//	GET    /schema
//	GET    /objects/{namespace}/{name}
//	GET    /objects/{namespace}
//	POST   /objects/{namespace}
//	PUT    /objects/{namespace}/{name}  [?forceAllowCreate=true]
//	DELETE /objects/{namespace}/{name}
//	GET    /watch/{namespace}         (text/event-stream)
//	POST   /validate
//	POST   /mutate
//
// Identity rides in X-Aggexp-User-{Name,Uid,Groups,Extra-*} headers;
// the "Extra-" keys are percent-encoded for characters disallowed in
// HTTP field names (mirrors the aggregation layer's X-Remote-Extra-*
// convention, per FINDINGS/0026). Watch responses are canonical SSE
// (`data: <json>\n\n`) with `:` comment keepalives.
//
// See FINDINGS/0026 for the transport-equivalence measurement at lab
// scale (HTTP ≈ gRPC for end-to-end latency; HTTP wins on toolchain
// footprint and debuggability).
package httpbackend

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	componentv2pb "github.com/cheeseandcereal/aggexp/runtime/component/v2/proto"
)

// Client implements componentv2pb.BackendClient over HTTP/JSON+SSE.
type Client struct {
	base *url.URL
	http *http.Client
}

// New builds a Client. addr may be bare host:port (assumed http://)
// or a full URL. Per-call deadlines come from ctx — the http.Client
// itself has no total-request timeout so Watch streams are not
// truncated.
func New(addr string, headerTimeout time.Duration) (*Client, error) {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid backend URL %q: %w", addr, err)
	}
	return &Client{
		base: u,
		http: &http.Client{
			Transport: &http.Transport{
				ResponseHeaderTimeout: headerTimeout,
				MaxIdleConnsPerHost:   16,
			},
		},
	}, nil
}

// --- componentv2pb.BackendClient implementation ---

func (c *Client) GetSchema(ctx context.Context, _ *componentv2pb.GetSchemaRequest, _ ...grpc.CallOption) (*componentv2pb.GetSchemaResponse, error) {
	body, _, err := c.doStatus(ctx, http.MethodGet, "/schema", nil, nil)
	if err != nil {
		return nil, err
	}
	var r httpSchemaResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "decode schema: %v", err)
	}
	cols := make([]*componentv2pb.TableColumn, 0, len(r.Columns))
	for _, col := range r.Columns {
		cols = append(cols, &componentv2pb.TableColumn{
			Name: col.Name, Type: col.Type, Format: col.Format,
			Description: col.Description, Priority: col.Priority,
		})
	}
	return &componentv2pb.GetSchemaResponse{
		Group: r.Group, Version: r.Version, Resource: r.Resource,
		Kind: r.Kind, Singular: r.Singular, Namespaced: r.Namespaced,
		Writable:                r.Writable,
		Schema:                  r.Schema,
		SchemaIsOpenapi:         r.SchemaIsOpenAPI,
		Columns:                 cols,
		RowFields:               r.RowFields,
		ShortNames:              r.ShortNames,
		Categories:              r.Categories,
		SupportsServerSideApply: r.SupportsServerSideApply,
		SupportsValidation:      r.SupportsValidation,
		SupportsMutation:        r.SupportsMutation,
		WatchCapability:         r.WatchCapability,
	}, nil
}

func (c *Client) Get(ctx context.Context, in *componentv2pb.GetRequest, _ ...grpc.CallOption) (*componentv2pb.GetResponse, error) {
	body, _, err := c.doStatus(ctx, http.MethodGet, fmt.Sprintf("/objects/%s/%s", in.Namespace, in.Name), in.User, nil)
	if err != nil {
		return nil, err
	}
	return &componentv2pb.GetResponse{ObjectJson: body}, nil
}

func (c *Client) List(ctx context.Context, in *componentv2pb.ListRequest, _ ...grpc.CallOption) (*componentv2pb.ListResponse, error) {
	body, _, err := c.doStatus(ctx, http.MethodGet, fmt.Sprintf("/objects/%s", in.Namespace), in.User, nil)
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "decode list: %v", err)
	}
	items := make([][]byte, len(list.Items))
	for i, it := range list.Items {
		items[i] = []byte(it)
	}
	return &componentv2pb.ListResponse{ItemsJson: items}, nil
}

func (c *Client) Create(ctx context.Context, in *componentv2pb.CreateRequest, _ ...grpc.CallOption) (*componentv2pb.CreateResponse, error) {
	body, _, err := c.doStatus(ctx, http.MethodPost, fmt.Sprintf("/objects/%s", in.Namespace), in.User, in.ObjectJson)
	if err != nil {
		return nil, err
	}
	return &componentv2pb.CreateResponse{ObjectJson: body}, nil
}

func (c *Client) Update(ctx context.Context, in *componentv2pb.UpdateRequest, _ ...grpc.CallOption) (*componentv2pb.UpdateResponse, error) {
	path := fmt.Sprintf("/objects/%s/%s", in.Namespace, in.Name)
	if in.ForceAllowCreate {
		path += "?forceAllowCreate=true"
	}
	body, status, err := c.doStatus(ctx, http.MethodPut, path, in.User, in.ObjectJson)
	if err != nil {
		return nil, err
	}
	return &componentv2pb.UpdateResponse{ObjectJson: body, Created: status == http.StatusCreated}, nil
}

func (c *Client) Apply(ctx context.Context, in *componentv2pb.ApplyRequest, _ ...grpc.CallOption) (*componentv2pb.ApplyResponse, error) {
	path := fmt.Sprintf("/objects/%s/%s?forceAllowCreate=true", in.Namespace, in.Name)
	body, status, err := c.doStatus(ctx, http.MethodPut, path, in.User, in.ObjectJson)
	if err != nil {
		return nil, err
	}
	return &componentv2pb.ApplyResponse{ObjectJson: body, Created: status == http.StatusCreated}, nil
}

func (c *Client) Delete(ctx context.Context, in *componentv2pb.DeleteRequest, _ ...grpc.CallOption) (*componentv2pb.DeleteResponse, error) {
	body, _, err := c.doStatus(ctx, http.MethodDelete, fmt.Sprintf("/objects/%s/%s", in.Namespace, in.Name), in.User, nil)
	if err != nil {
		return nil, err
	}
	return &componentv2pb.DeleteResponse{ObjectJson: body, Deleted: true}, nil
}

func (c *Client) Watch(ctx context.Context, in *componentv2pb.WatchRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[componentv2pb.WatchEvent], error) {
	u := *c.base
	u.Path = fmt.Sprintf("/watch/%s", in.Namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	applyUserHeaders(req, in.User)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Unavailable, "watch dial: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, translateHTTPStatus(resp.StatusCode, mustReadMessage(resp.Body))
	}
	return newSSEStream(ctx, resp), nil
}

func (c *Client) Validate(ctx context.Context, in *componentv2pb.ValidateRequest, _ ...grpc.CallOption) (*componentv2pb.ValidateResponse, error) {
	payload := map[string]any{
		"namespace":     in.Namespace,
		"name":          in.Name,
		"operation":     in.Operation,
		"object":        json.RawMessage(in.ObjectJson),
		"oldObject":     maybeJSON(in.OldObjectJson),
	}
	raw, _ := json.Marshal(payload)
	body, _, err := c.doStatus(ctx, http.MethodPost, "/validate", in.User, raw)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Allowed bool   `json:"allowed"`
		Reason  string `json:"reason,omitempty"`
		Causes  []struct {
			Field   string `json:"field"`
			Message string `json:"message"`
			Type    string `json:"type,omitempty"`
		} `json:"causes,omitempty"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "decode validate: %v", err)
	}
	out := &componentv2pb.ValidateResponse{Allowed: resp.Allowed, Reason: resp.Reason}
	for _, c := range resp.Causes {
		out.Causes = append(out.Causes, &componentv2pb.AdmissionCause{
			Field: c.Field, Message: c.Message, Type: c.Type,
		})
	}
	return out, nil
}

func (c *Client) Mutate(ctx context.Context, in *componentv2pb.MutateRequest, _ ...grpc.CallOption) (*componentv2pb.MutateResponse, error) {
	payload := map[string]any{
		"namespace": in.Namespace,
		"name":      in.Name,
		"operation": in.Operation,
		"object":    json.RawMessage(in.ObjectJson),
		"oldObject": maybeJSON(in.OldObjectJson),
	}
	raw, _ := json.Marshal(payload)
	body, _, err := c.doStatus(ctx, http.MethodPost, "/mutate", in.User, raw)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Mutated json.RawMessage `json:"mutatedObject,omitempty"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "decode mutate: %v", err)
	}
	return &componentv2pb.MutateResponse{MutatedObjectJson: resp.Mutated}, nil
}

// --- HTTP helpers ---

type httpSchemaResponse struct {
	Group                   string          `json:"group"`
	Version                 string          `json:"version"`
	Resource                string          `json:"resource"`
	Kind                    string          `json:"kind"`
	Singular                string          `json:"singular"`
	Namespaced              bool            `json:"namespaced"`
	Writable                bool            `json:"writable"`
	SchemaIsOpenAPI         bool            `json:"schemaIsOpenapi,omitempty"`
	Schema                  json.RawMessage `json:"schema,omitempty"`
	Columns                 []struct {
		Name        string `json:"name"`
		Type        string `json:"type"`
		Format      string `json:"format,omitempty"`
		Description string `json:"description,omitempty"`
		Priority    int32  `json:"priority,omitempty"`
	} `json:"columns,omitempty"`
	RowFields               []string `json:"rowFields,omitempty"`
	ShortNames              []string `json:"shortNames,omitempty"`
	Categories              []string `json:"categories,omitempty"`
	SupportsServerSideApply bool     `json:"supportsServerSideApply,omitempty"`
	SupportsValidation      bool     `json:"supportsValidation,omitempty"`
	SupportsMutation        bool     `json:"supportsMutation,omitempty"`
	WatchCapability         string   `json:"watchCapability,omitempty"`
}

func (c *Client) doStatus(ctx context.Context, method, path string, user *componentv2pb.UserInfo, body []byte) ([]byte, int, error) {
	u := *c.base
	if i := strings.IndexByte(path, '?'); i >= 0 {
		u.Path = path[:i]
		u.RawQuery = path[i+1:]
	} else {
		u.Path = path
	}
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bodyReader)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	applyUserHeaders(req, user)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, grpcstatus.Errorf(codes.Unavailable, "http %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, grpcstatus.Errorf(codes.Internal, "read response: %v", err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return respBody, resp.StatusCode, nil
	}
	return nil, resp.StatusCode, translateHTTPStatus(resp.StatusCode, extractMessage(respBody))
}

func applyUserHeaders(req *http.Request, user *componentv2pb.UserInfo) {
	if user == nil {
		return
	}
	if user.Name != "" {
		req.Header.Set("X-Aggexp-User-Name", user.Name)
	}
	if user.Uid != "" {
		req.Header.Set("X-Aggexp-User-Uid", user.Uid)
	}
	if len(user.Groups) > 0 {
		req.Header.Set("X-Aggexp-User-Groups", strings.Join(user.Groups, ","))
	}
	for k, v := range user.Extra {
		if v == nil || len(v.Values) == 0 {
			continue
		}
		req.Header.Set("X-Aggexp-User-Extra-"+headerEscapeExtraKey(k), strings.Join(v.Values, ","))
	}
}

func translateHTTPStatus(status int, msg string) error {
	var code codes.Code
	switch status {
	case http.StatusNotFound:
		code = codes.NotFound
	case http.StatusConflict:
		code = codes.AlreadyExists
	case http.StatusBadRequest:
		code = codes.InvalidArgument
	case http.StatusForbidden:
		code = codes.PermissionDenied
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
		code = codes.Unavailable
	default:
		code = codes.Unknown
	}
	if msg == "" {
		msg = fmt.Sprintf("backend returned HTTP %d", status)
	}
	return grpcstatus.Error(code, msg)
}

func headerEscapeExtraKey(k string) string {
	var sb strings.Builder
	sb.Grow(len(k))
	for _, c := range []byte(k) {
		switch {
		case (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'):
			sb.WriteByte(c)
		case c == '!' || c == '#' || c == '$' || c == '&' ||
			c == '\'' || c == '*' || c == '+' || c == '-' ||
			c == '.' || c == '^' || c == '_' || c == '`' ||
			c == '|' || c == '~':
			sb.WriteByte(c)
		default:
			fmt.Fprintf(&sb, "%%%02X", c)
		}
	}
	return sb.String()
}

func extractMessage(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var e struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &e); err == nil && e.Message != "" {
		return e.Message
	}
	return strings.TrimSpace(string(body))
}

func mustReadMessage(r io.Reader) string {
	b, err := io.ReadAll(r)
	if err != nil {
		return ""
	}
	return extractMessage(b)
}

func maybeJSON(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return json.RawMessage(raw)
}

// --- SSE stream ---

type sseStream struct {
	ctx    context.Context
	resp   *http.Response
	reader *bufio.Reader
}

func newSSEStream(ctx context.Context, resp *http.Response) *sseStream {
	r := bufio.NewReaderSize(resp.Body, 1<<20)
	return &sseStream{ctx: ctx, resp: resp, reader: r}
}

// Recv reads the next SSE event and parses it as a WatchEvent.
func (s *sseStream) Recv() (*componentv2pb.WatchEvent, error) {
	var dataBuf bytes.Buffer
	for {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
		line, err := s.reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && dataBuf.Len() == 0 && len(line) == 0 {
				return nil, io.EOF
			}
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "" {
			if dataBuf.Len() == 0 {
				if errors.Is(err, io.EOF) {
					return nil, io.EOF
				}
				continue
			}
			return parseWatchEventData(dataBuf.Bytes())
		}
		if strings.HasPrefix(trimmed, ":") {
			continue
		}
		if strings.HasPrefix(trimmed, "data:") {
			payload := strings.TrimPrefix(trimmed, "data:")
			payload = strings.TrimPrefix(payload, " ")
			if dataBuf.Len() > 0 {
				dataBuf.WriteByte('\n')
			}
			dataBuf.WriteString(payload)
			continue
		}
	}
}

func parseWatchEventData(raw []byte) (*componentv2pb.WatchEvent, error) {
	var wrapped struct {
		Type   string          `json:"type"`
		Object json.RawMessage `json:"object"`
	}
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		return nil, fmt.Errorf("decode SSE event: %w", err)
	}
	var t componentv2pb.EventType
	switch strings.ToUpper(wrapped.Type) {
	case "ADDED":
		t = componentv2pb.EventType_EVENT_ADDED
	case "MODIFIED":
		t = componentv2pb.EventType_EVENT_MODIFIED
	case "DELETED":
		t = componentv2pb.EventType_EVENT_DELETED
	case "BOOKMARK":
		t = componentv2pb.EventType_EVENT_BOOKMARK
	default:
		t = componentv2pb.EventType_EVENT_UNSPECIFIED
	}
	return &componentv2pb.WatchEvent{Type: t, ObjectJson: []byte(wrapped.Object)}, nil
}

func (s *sseStream) Context() context.Context             { return s.ctx }
func (s *sseStream) Header() (metadata.MD, error)         { return nil, nil }
func (s *sseStream) Trailer() metadata.MD                 { return nil }
func (s *sseStream) CloseSend() error                     { return s.resp.Body.Close() }
func (s *sseStream) SendMsg(_ any) error                  { return fmt.Errorf("SendMsg not supported on SSE stream") }
func (s *sseStream) RecvMsg(m any) error {
	ev, err := s.Recv()
	if err != nil {
		return err
	}
	out, ok := m.(*componentv2pb.WatchEvent)
	if !ok {
		return fmt.Errorf("RecvMsg: unexpected type %T", m)
	}
	out.Type = ev.Type
	out.ObjectJson = ev.ObjectJson
	return nil
}

var (
	_ componentv2pb.BackendClient                             = (*Client)(nil)
	_ grpc.ServerStreamingClient[componentv2pb.WatchEvent]    = (*sseStream)(nil)
)
