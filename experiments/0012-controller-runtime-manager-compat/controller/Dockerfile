# syntax=docker/dockerfile:1.7
# Self-contained controller build. No repo-root context needed; the
# experiment deliberately owns its go.mod.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/controller ./...

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/controller /controller
USER nonroot:nonroot
ENTRYPOINT ["/controller"]
