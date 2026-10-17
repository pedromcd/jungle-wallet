# syntax=docker/dockerfile:1
# keep in sync with the "go" directive of go.mod (the version all tests were run with)
ARG GO_VERSION=1.22

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/walletd ./cmd/walletd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/walletd /walletd
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/walletd"]
