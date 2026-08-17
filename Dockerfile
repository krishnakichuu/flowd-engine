# flowd is a self-contained static binary — migrations (migrations/migrations.go)
# and the web dashboard (internal/webui/embed.go) are both go:embed'd, so the
# runtime stage needs nothing but the compiled binary.

FROM golang:1.25-alpine AS builder
ARG VERSION=dev
WORKDIR /src

COPY go.work go.work.sum ./
COPY go.mod go.sum ./
COPY sdk/go.mod sdk/go.sum ./sdk/
COPY tools/go.mod tools/go.sum ./tools/
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/flowd ./cmd/flowd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/flowd /flowd
USER nonroot:nonroot
EXPOSE 7233 7234 9090
ENTRYPOINT ["/flowd"]
