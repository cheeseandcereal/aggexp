# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/aggexp-hello ./cmd/aggexp-hello

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/aggexp-hello /aggexp-hello
USER nonroot:nonroot
ENTRYPOINT ["/aggexp-hello"]
