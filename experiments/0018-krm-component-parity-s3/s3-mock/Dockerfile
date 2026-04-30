# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/s3-mock .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/s3-mock /s3-mock
USER nonroot:nonroot
ENTRYPOINT ["/s3-mock"]
