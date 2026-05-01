# syntax=docker/dockerfile:1.7
# Build from the repo root so the component, backend, and s3-mock all
# use consistent context conventions:
#   docker build -f experiments/0024-metadata-crd-store/s3-mock/Dockerfile \
#       -t aggexp-0024-s3-mock:dev .
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY experiments/0024-metadata-crd-store/s3-mock/ /src/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/s3-mock .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/s3-mock /s3-mock
USER nonroot:nonroot
ENTRYPOINT ["/s3-mock"]
