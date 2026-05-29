# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
# Build context must be the repo root so the experiment's `replace`
# directive can resolve ../.. when compiling.
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0048-library-multireplica-vertical-slice/ /src/experiments/0048-library-multireplica-vertical-slice/
WORKDIR /src/experiments/0048-library-multireplica-vertical-slice
# GOTOOLCHAIN=local pins to the image's go 1.24 (FINDINGS/0046: a newer
# toolchain silently bumps the go directive and breaks the build).
ENV GOTOOLCHAIN=local
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/aggexp-widgets ./cmd/aggexp-widgets

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/aggexp-widgets /aggexp-widgets
USER nonroot:nonroot
ENTRYPOINT ["/aggexp-widgets"]
