# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
# Copy root module sources so the experiment's replace directive can
# resolve ../.. when building.
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0007-runtime-fs-driver/ /src/experiments/0007-runtime-fs-driver/
WORKDIR /src/experiments/0007-runtime-fs-driver
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/aggexp-files ./cmd/aggexp-files

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/aggexp-files /aggexp-files
USER nonroot:nonroot
ENTRYPOINT ["/aggexp-files"]
