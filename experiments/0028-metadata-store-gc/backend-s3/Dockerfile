# syntax=docker/dockerfile:1.7
# Build from repo root context:
#   docker build -f experiments/0028-metadata-store-gc/backend-s3/Dockerfile \
#       -t aggexp-0028-backend-s3:dev .
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0028-metadata-store-gc/ /src/experiments/0028-metadata-store-gc/
WORKDIR /src/experiments/0028-metadata-store-gc
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/backend-s3 ./backend-s3/cmd/backend-s3

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/backend-s3 /backend-s3
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/backend-s3", "--addr=0.0.0.0:9090"]
