# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mock-github .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/mock-github /mock-github
USER nonroot:nonroot
ENTRYPOINT ["/mock-github"]
