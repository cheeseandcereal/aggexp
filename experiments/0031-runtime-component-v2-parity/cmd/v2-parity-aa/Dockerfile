# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
# The experiment module's go.mod has `replace ... => ../..` for the
# substrate, so we copy both the root and the experiment dir.
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0031-runtime-component-v2-parity/ /src/experiments/0031-runtime-component-v2-parity/
WORKDIR /src/experiments/0031-runtime-component-v2-parity
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/v2-parity-aa ./cmd/v2-parity-aa

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/v2-parity-aa /v2-parity-aa
USER nonroot:nonroot
ENTRYPOINT ["/v2-parity-aa"]
