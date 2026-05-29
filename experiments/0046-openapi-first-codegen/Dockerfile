# syntax=docker/dockerfile:1.7
# Build context MUST be the repo root so the experiment's `replace`
# directive (github.com/cheeseandcereal/aggexp => ../..) resolves at
# compile time. See 0034's Dockerfile for the same pattern.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum /src/
COPY runtime/ /src/runtime/
COPY drivers/ /src/drivers/
COPY experiments/0046-openapi-first-codegen/ /src/experiments/0046-openapi-first-codegen/
WORKDIR /src/experiments/0046-openapi-first-codegen
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/verify-aa ./cmd/verify-aa

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/verify-aa /verify-aa
USER nonroot:nonroot
ENTRYPOINT ["/verify-aa"]
