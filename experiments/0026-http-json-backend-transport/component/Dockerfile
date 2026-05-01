# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0026-http-json-backend-transport/ /src/experiments/0026-http-json-backend-transport/
WORKDIR /src/experiments/0026-http-json-backend-transport
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/note-aa-http ./component

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/note-aa-http /note-aa-http
USER nonroot:nonroot
ENTRYPOINT ["/note-aa-http"]
