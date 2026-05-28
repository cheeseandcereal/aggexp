FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY ../../go.mod ../../go.sum /src-root/
COPY ../../runtime/ /src-root/runtime/
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /widget-aa ./cmd/widget-aa

FROM gcr.io/distroless/static:nonroot
COPY --from=build /widget-aa /widget-aa
ENTRYPOINT ["/widget-aa"]
