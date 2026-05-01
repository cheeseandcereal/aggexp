// HTTP/JSON transport adapter — implements
// runtime/component/proto.BackendClient over plain HTTP. Watch
// responses arrive as SSE and are adapted to the
// grpc.ServerStreamingClient[WatchEvent] shape the existing REST
// storage expects.
//
// This file is ~350 lines and is the load-bearing piece of the
// experiment: everything above it in the component (scheme,
// grpcbackend.REST, openapi) stays unchanged from the gRPC path.

package main

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

	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
)

// httpClient speaks HTTP/JSON+SSE but presents as
// componentpb.BackendClient so runtime/component/grpcbackend is
// reusable verbatim.
type httpClient struct {
	base *url.URL
	http *http.Client
}

func newHTTPBackendClient(addr string, timeout time.Duration) *httpClient {
	// Accept bare host:port (assume http://) for symmetry with the
	// grpc flag's shape.
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		// Flag validation should have caught this; panic is fine
		// inside startup.
		panic("invalid --backend-addr for http transport: " + err.Error())
	}
	return &httpClient{
		base: u,
		http: &http.Client{
			// No Timeout on the client itself — that would kill
			// long-running Watch streams. Per-call deadlines come
			// from the ctx.
			Transport: &http.Transport{
				ResponseHeaderTimeout: timeout,
				MaxIdleConnsPerHost:   16,
			},
		},
	}
}

// ----- componentpb.BackendClient implementation -----------------------------

func (c *httpClient) GetSchema(ctx context.Context, _ *componentpb.GetSchemaRequest, _ ...grpc.CallOption) (*componentpb.GetSchemaResponse, error) {
	body, err := c.do(ctx, http.MethodGet, "/schema", nil, nil)
	if err != nil {
		return nil, err
	}
	var r httpSchemaResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "decode schema: %v", err)
	}
	cols := make([]*componentpb.TableColumn, 0, len(r.Columns))
	for _, c := range r.Columns {
		cols = append(cols, &componentpb.TableColumn{
			Name:        c.Name,
			Type:        c.Type,
			Format:      c.Format,
			Description: c.Description,
			Priority:    c.Priority,
		})
	}
	return &componentpb.GetSchemaResponse{
		Group:                   r.Group,
		Version:                 r.Version,
		Resource:                r.Resource,
		Kind:                    r.Kind,
		Singular:                r.Singular,
		Namespaced:              r.Namespaced,
		Writable:                r.Writable,
		OpenapiV3:               r.OpenAPIV3,
		Columns:                 cols,
		RowFields:               r.RowFields,
		ShortNames:              r.ShortNames,
		Categories:              r.Categories,
		SupportsServerSideApply: r.SupportsServerSideApply,
	}, nil
}

func (c *httpClient) Get(ctx context.Context, in *componentpb.GetRequest, _ ...grpc.CallOption) (*componentpb.GetResponse, error) {
	body, err := c.do(ctx, http.MethodGet,
		fmt.Sprintf("/objects/%s/%s", in.Namespace, in.Name),
		in.User, nil)
	if err != nil {
		return nil, err
	}
	return &componentpb.GetResponse{ObjectJson: body}, nil
}

func (c *httpClient) List(ctx context.Context, in *componentpb.ListRequest, _ ...grpc.CallOption) (*componentpb.ListResponse, error) {
	body, err := c.do(ctx, http.MethodGet,
		fmt.Sprintf("/objects/%s", in.Namespace),
		in.User, nil)
	if err != nil {
		return nil, err
	}
	var list struct {
		Items           []json.RawMessage `json:"items"`
		ResourceVersion string            `json:"resourceVersion,omitempty"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "decode list: %v", err)
	}
	items := make([][]byte, len(list.Items))
	for i, it := range list.Items {
		items[i] = []byte(it)
	}
	return &componentpb.ListResponse{
		ItemsJson:       items,
		ResourceVersion: list.ResourceVersion,
	}, nil
}

func (c *httpClient) Create(ctx context.Context, in *componentpb.CreateRequest, _ ...grpc.CallOption) (*componentpb.CreateResponse, error) {
	body, err := c.do(ctx, http.MethodPost,
		fmt.Sprintf("/objects/%s", in.Namespace),
		in.User, in.ObjectJson)
	if err != nil {
		return nil, err
	}
	return &componentpb.CreateResponse{ObjectJson: body}, nil
}

func (c *httpClient) Update(ctx context.Context, in *componentpb.UpdateRequest, _ ...grpc.CallOption) (*componentpb.UpdateResponse, error) {
	path := fmt.Sprintf("/objects/%s/%s", in.Namespace, in.Name)
	if in.ForceAllowCreate {
		path += "?forceAllowCreate=true"
	}
	// Backend's update-or-upsert returns 201 for create, 200 for
	// update; we surface `created` via the status code.
	body, status, err := c.doStatus(ctx, http.MethodPut, path, in.User, in.ObjectJson)
	if err != nil {
		return nil, err
	}
	return &componentpb.UpdateResponse{
		ObjectJson: body,
		Created:    status == http.StatusCreated,
	}, nil
}

// Apply maps to Update with forceAllowCreate. The component's SSA
// path always computes the merged object; the backend persists it
// with Update semantics. Mirrors the note-backend-py behavior in
// 0019 and the 0017 Go reference.
func (c *httpClient) Apply(ctx context.Context, in *componentpb.ApplyRequest, _ ...grpc.CallOption) (*componentpb.ApplyResponse, error) {
	path := fmt.Sprintf("/objects/%s/%s?forceAllowCreate=true", in.Namespace, in.Name)
	body, status, err := c.doStatus(ctx, http.MethodPut, path, in.User, in.ObjectJson)
	if err != nil {
		return nil, err
	}
	return &componentpb.ApplyResponse{
		ObjectJson: body,
		Created:    status == http.StatusCreated,
	}, nil
}

func (c *httpClient) Delete(ctx context.Context, in *componentpb.DeleteRequest, _ ...grpc.CallOption) (*componentpb.DeleteResponse, error) {
	body, err := c.do(ctx, http.MethodDelete,
		fmt.Sprintf("/objects/%s/%s", in.Namespace, in.Name),
		in.User, nil)
	if err != nil {
		return nil, err
	}
	return &componentpb.DeleteResponse{ObjectJson: body, Deleted: true}, nil
}

func (c *httpClient) Watch(ctx context.Context, in *componentpb.WatchRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[componentpb.WatchEvent], error) {
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

// ----- HTTP helpers ---------------------------------------------------------

// httpSchemaResponse mirrors backend's schemaResponse. We duplicate
// rather than importing so the component and the backend share no
// build-time code.
type httpSchemaResponse struct {
	Group                   string          `json:"group"`
	Version                 string          `json:"version"`
	Resource                string          `json:"resource"`
	Kind                    string          `json:"kind"`
	Singular                string          `json:"singular"`
	Namespaced              bool            `json:"namespaced"`
	Writable                bool            `json:"writable"`
	SupportsServerSideApply bool            `json:"supportsServerSideApply"`
	OpenAPIV3               json.RawMessage `json:"openapiV3"`
	Columns                 []struct {
		Name        string `json:"name"`
		Type        string `json:"type"`
		Format      string `json:"format,omitempty"`
		Description string `json:"description,omitempty"`
		Priority    int32  `json:"priority,omitempty"`
	} `json:"columns,omitempty"`
	RowFields  []string `json:"rowFields,omitempty"`
	ShortNames []string `json:"shortNames,omitempty"`
	Categories []string `json:"categories,omitempty"`
}

func (c *httpClient) do(ctx context.Context, method, path string, user *componentpb.UserInfo, body []byte) ([]byte, error) {
	b, _, err := c.doStatus(ctx, method, path, user, body)
	return b, err
}

func (c *httpClient) doStatus(ctx context.Context, method, path string, user *componentpb.UserInfo, body []byte) ([]byte, int, error) {
	u := *c.base
	// path may include a query string.
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

// applyUserHeaders stamps identity onto the HTTP request. The backend
// is free to ignore these; the middleware stamps them unconditionally
// so backends that care (audit loggers, per-caller rate limits) have
// them available.
func applyUserHeaders(req *http.Request, user *componentpb.UserInfo) {
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
		// HTTP header field names must match the `tchar` set (no "/",
		// no "%", no ",", etc). The aggregation layer's X-Remote-Extra-*
		// convention uses URL-escaping for "/"; mirror that behavior
		// here. Backends wanting the raw key can URL-decode after
		// stripping the X-Aggexp-User-Extra- prefix.
		escaped := headerEscapeExtraKey(k)
		req.Header.Set("X-Aggexp-User-Extra-"+escaped, strings.Join(v.Values, ","))
	}
}

// translateHTTPStatus maps HTTP status codes to gRPC codes so the
// existing grpcbackend.REST error translation (which keys on gRPC
// codes) continues to work unchanged.
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

// headerEscapeExtraKey replaces characters disallowed in HTTP header
// field names with their percent-encoded form. The allowed set is the
// `tchar` production from RFC 7230 (alphanumerics and `!#$%&'*+-.^_`~|`);
// anything else — notably `/`, `,`, `;`, `=` — is percent-encoded. This
// matches the `%2F` escaping the aggregation layer uses on
// X-Remote-Extra-* headers.
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

// ----- SSE stream -----------------------------------------------------------

// sseStream adapts a text/event-stream body into the
// grpc.ServerStreamingClient[componentpb.WatchEvent] interface.
// Only Recv and Context are genuinely used by grpcbackend.REST;
// the other ClientStream methods are stubbed.
type sseStream struct {
	ctx    context.Context
	resp   *http.Response
	reader *bufio.Reader
}

func newSSEStream(ctx context.Context, resp *http.Response) *sseStream {
	// Large buffer: some watch events (objects with managedFields
	// or big status blobs) can exceed bufio's default 4KB line
	// buffer. 1MB is generous and matches the aggregation layer's
	// max-request-body default.
	r := bufio.NewReaderSize(resp.Body, 1<<20)
	return &sseStream{ctx: ctx, resp: resp, reader: r}
}

// Recv reads the next SSE event and returns its parsed WatchEvent.
// Returns io.EOF when the stream closes cleanly.
//
// SSE framing (RFC: Server-Sent Events): events are separated by
// one blank line. Each event is a sequence of lines; lines beginning
// with "data:" contribute to the event data. We accumulate all
// `data:` lines, strip the prefix and at most one leading space,
// then JSON-decode the concatenation.
func (s *sseStream) Recv() (*componentpb.WatchEvent, error) {
	var dataBuf bytes.Buffer
	for {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
		line, err := s.reader.ReadString('\n')
		if err != nil {
			// Incomplete final chunk with err==io.EOF: if we have
			// no buffered data, surface EOF; otherwise process.
			if errors.Is(err, io.EOF) && dataBuf.Len() == 0 && len(line) == 0 {
				return nil, io.EOF
			}
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
		}
		// Strip trailing \n (and \r if CRLF).
		trimmed := strings.TrimRight(line, "\r\n")

		if trimmed == "" {
			// End of event. If we accumulated nothing, keep
			// reading (comment lines / keepalives).
			if dataBuf.Len() == 0 {
				if errors.Is(err, io.EOF) {
					return nil, io.EOF
				}
				continue
			}
			return parseWatchEventData(dataBuf.Bytes())
		}
		// Comment line per SSE spec; ignore.
		if strings.HasPrefix(trimmed, ":") {
			continue
		}
		if strings.HasPrefix(trimmed, "data:") {
			payload := strings.TrimPrefix(trimmed, "data:")
			// SSE spec: strip exactly one leading space.
			payload = strings.TrimPrefix(payload, " ")
			if dataBuf.Len() > 0 {
				dataBuf.WriteByte('\n')
			}
			dataBuf.WriteString(payload)
			continue
		}
		// Other SSE fields (event:, id:, retry:) are legal per
		// spec but unused here; ignore for forward-compat.
	}
}

// parseWatchEventData decodes the backend's watchEvent JSON (type +
// object) into a componentpb.WatchEvent.
func parseWatchEventData(raw []byte) (*componentpb.WatchEvent, error) {
	var wrapped struct {
		Type   string          `json:"type"`
		Object json.RawMessage `json:"object"`
	}
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		return nil, fmt.Errorf("decode SSE event: %w", err)
	}
	var t componentpb.EventType
	switch strings.ToUpper(wrapped.Type) {
	case "ADDED":
		t = componentpb.EventType_EVENT_ADDED
	case "MODIFIED":
		t = componentpb.EventType_EVENT_MODIFIED
	case "DELETED":
		t = componentpb.EventType_EVENT_DELETED
	case "BOOKMARK":
		t = componentpb.EventType_EVENT_BOOKMARK
	default:
		t = componentpb.EventType_EVENT_UNSPECIFIED
	}
	return &componentpb.WatchEvent{Type: t, ObjectJson: []byte(wrapped.Object)}, nil
}

// ClientStream surface: only Context is genuinely consulted by
// grpcbackend.REST (via the stream's ctx when closing). The rest
// are unused but required by the interface.

func (s *sseStream) Context() context.Context { return s.ctx }
func (s *sseStream) Header() (metadata.MD, error) {
	return nil, nil
}
func (s *sseStream) Trailer() metadata.MD { return nil }
func (s *sseStream) CloseSend() error {
	return s.resp.Body.Close()
}
func (s *sseStream) SendMsg(m any) error {
	return fmt.Errorf("SendMsg not supported on server-streaming SSE")
}
func (s *sseStream) RecvMsg(m any) error {
	ev, err := s.Recv()
	if err != nil {
		return err
	}
	out, ok := m.(*componentpb.WatchEvent)
	if !ok {
		return fmt.Errorf("RecvMsg: unexpected type %T", m)
	}
	*out = *ev
	return nil
}

// Compile-time interface assertion: httpClient IS a BackendClient.
var _ componentpb.BackendClient = (*httpClient)(nil)
var _ grpc.ServerStreamingClient[componentpb.WatchEvent] = (*sseStream)(nil)
