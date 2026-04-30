# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY experiments/0017-krm-protocol-refinement/gen/ /src/experiments/0017-krm-protocol-refinement/gen/
COPY experiments/0017-krm-protocol-refinement/backend-note/ /src/experiments/0017-krm-protocol-refinement/backend-note/
WORKDIR /src/experiments/0017-krm-protocol-refinement/backend-note
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/note-backend ./cmd/note-backend

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/note-backend /note-backend
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/note-backend", "--addr=0.0.0.0:9090"]
