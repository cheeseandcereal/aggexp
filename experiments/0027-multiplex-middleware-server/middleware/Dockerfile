# The 0027 multiplex middleware binary. Built against the root
# aggexp module — uses runtime/component/{scheme,openapi,grpcbackend},
# runtime/group, runtime/server from the substrate, plus new
# per-experiment code under this folder.
FROM golang:1.24-alpine AS build
WORKDIR /src
# Copy the entire repo so module paths resolve. Multi-stage build
# copies out only the binary.
COPY go.mod go.sum ./
COPY runtime/ runtime/
COPY experiments/0027-multiplex-middleware-server/middleware/ experiments/0027-multiplex-middleware-server/middleware/
RUN CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' \
    -o /out/aggexp-multiplex \
    ./experiments/0027-multiplex-middleware-server/middleware

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/aggexp-multiplex /aggexp-multiplex
ENTRYPOINT ["/aggexp-multiplex"]
