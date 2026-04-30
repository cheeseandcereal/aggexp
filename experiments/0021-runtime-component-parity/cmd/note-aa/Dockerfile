# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
# note-aa depends on the root module (substrate) and the experiment
# module. Copy both; the experiment's go.mod's replace directive
# points at ../.. for the substrate.
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0021-runtime-component-parity/ /src/experiments/0021-runtime-component-parity/
WORKDIR /src/experiments/0021-runtime-component-parity
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/note-aa ./cmd/note-aa

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/note-aa /note-aa
USER nonroot:nonroot
ENTRYPOINT ["/note-aa"]
