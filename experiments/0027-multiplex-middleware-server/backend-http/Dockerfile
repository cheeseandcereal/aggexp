# Stdlib-only HTTP backend for experiment 0027. One image, three
# pod specs — RESOURCE/PLURAL/KIND/GROUP env vars differentiate.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o /out/backend-http .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/backend-http /backend-http
ENTRYPOINT ["/backend-http"]
