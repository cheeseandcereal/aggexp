# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
# Backend imports 0013's gen/ module; copy both.
COPY experiments/0013-krm-component-skeleton/gen/ /src/experiments/0013-krm-component-skeleton/gen/
COPY experiments/0018-krm-component-parity-s3/backend-s3/ /src/experiments/0018-krm-component-parity-s3/backend-s3/
WORKDIR /src/experiments/0018-krm-component-parity-s3/backend-s3
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/backend-s3 ./cmd/backend-s3

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/backend-s3 /backend-s3
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/backend-s3", "--addr=0.0.0.0:9090"]
