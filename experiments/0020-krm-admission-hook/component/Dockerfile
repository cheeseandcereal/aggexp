# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
# Component depends on the root module (via the substrate) and the
# experiment's gen module. Copy what we need.
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0020-krm-admission-hook/gen/ /src/experiments/0020-krm-admission-hook/gen/
COPY experiments/0020-krm-admission-hook/component/ /src/experiments/0020-krm-admission-hook/component/
WORKDIR /src/experiments/0020-krm-admission-hook/component
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/aggexp-krm-component ./cmd/aggexp-krm-component

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/aggexp-krm-component /aggexp-krm-component
USER nonroot:nonroot
ENTRYPOINT ["/aggexp-krm-component"]
