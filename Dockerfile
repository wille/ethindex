FROM golang:1.26 AS builder

LABEL org.opencontainers.image.source=https://github.com/wille/ethindex

ARG TARGETOS
ARG TARGETARCH

WORKDIR /ethindex

COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -o ethindex ./cmd/ethindex

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /ethindex/ethindex .

ENTRYPOINT ["/ethindex"]
CMD ["-config", "/etc/ethindex/config.yaml"]
