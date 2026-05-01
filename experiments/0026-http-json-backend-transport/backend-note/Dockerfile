# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
# Backend is in its own tiny module (stdlib-only). Build from its root.
COPY experiments/0026-http-json-backend-transport/backend-note/ /src/
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/note-backend-http .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/note-backend-http /note-backend-http
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/note-backend-http", "--addr=0.0.0.0:8080"]
