# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
# backend-note imports runtime/component/proto from the substrate.
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY experiments/0021-runtime-component-parity/ /src/experiments/0021-runtime-component-parity/
WORKDIR /src/experiments/0021-runtime-component-parity
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/note-backend ./backend-note/cmd/note-backend

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/note-backend /note-backend
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/note-backend", "--addr=0.0.0.0:9090"]
