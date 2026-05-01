# syntax=docker/dockerfile:1.7
# Build from repo root context:
#   docker build -f experiments/0024-metadata-crd-store/component/Dockerfile \
#       -t aggexp-0024-component:dev .
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0024-metadata-crd-store/ /src/experiments/0024-metadata-crd-store/
WORKDIR /src/experiments/0024-metadata-crd-store
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/component ./component/cmd/aggexp-metadata-crd-component

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/component /aggexp-component
USER nonroot:nonroot
ENTRYPOINT ["/aggexp-component"]
