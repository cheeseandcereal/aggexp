# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
# Copy root module sources so the experiment's replace directive can
# resolve ../.. when building.
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0009-ack-aggregated-s3/ /src/experiments/0009-ack-aggregated-s3/
WORKDIR /src/experiments/0009-ack-aggregated-s3
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/aggexp-s3 ./cmd/aggexp-s3

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/aggexp-s3 /aggexp-s3
USER nonroot:nonroot
ENTRYPOINT ["/aggexp-s3"]
