# syntax=docker/dockerfile:1.7
# Build from repo root context:
#   docker build -f experiments/0028-metadata-store-gc/component/Dockerfile \
#       -t aggexp-0028-component:dev .
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0028-metadata-store-gc/ /src/experiments/0028-metadata-store-gc/
WORKDIR /src/experiments/0028-metadata-store-gc
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/component ./component/cmd/aggexp-metadata-store-gc-component

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/component /aggexp-component
USER nonroot:nonroot
ENTRYPOINT ["/aggexp-component"]
