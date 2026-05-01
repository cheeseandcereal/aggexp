# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0025-push-backed-watch/ /src/experiments/0025-push-backed-watch/
WORKDIR /src/experiments/0025-push-backed-watch
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/note-backend-push ./backend-note-push

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/note-backend-push /note-backend-push
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/note-backend-push", "--addr=0.0.0.0:9090"]
