# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0023-schema-source-exploration/ /src/experiments/0023-schema-source-exploration/
WORKDIR /src/experiments/0023-schema-source-exploration
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/note-aa ./track-b-middleware-synthesis/component

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/note-aa /note-aa
USER nonroot:nonroot
ENTRYPOINT ["/note-aa"]
