# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
# Build context must be the repo root so the experiment's `replace`
# directive can resolve ../.. when compiling.
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0045-read-path-reconcile-amplification/ /src/experiments/0045-read-path-reconcile-amplification/
WORKDIR /src/experiments/0045-read-path-reconcile-amplification
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/aggexp-widgets ./cmd/aggexp-widgets

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/aggexp-widgets /aggexp-widgets
USER nonroot:nonroot
ENTRYPOINT ["/aggexp-widgets"]
