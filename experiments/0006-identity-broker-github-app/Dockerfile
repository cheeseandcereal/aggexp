# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/aggexp-repos ./cmd/aggexp-repos

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/aggexp-repos /aggexp-repos
USER nonroot:nonroot
ENTRYPOINT ["/aggexp-repos"]
