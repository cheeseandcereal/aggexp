# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0023-schema-source-exploration/ /src/experiments/0023-schema-source-exploration/
WORKDIR /src/experiments/0023-schema-source-exploration
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/note-backend ./track-a-backend-openapi/backend-note

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/note-backend /note-backend
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/note-backend", "--addr=0.0.0.0:9090"]
