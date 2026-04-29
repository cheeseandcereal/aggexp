# Phase 0 probe image.
# Stdlib-only Go program; CGO off so the distroless static base works.

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/probe .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/probe /probe
USER nonroot:nonroot
ENTRYPOINT ["/probe"]
