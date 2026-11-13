# ── Build stage ──────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder
WORKDIR /app

# System deps for patch (needed for gocql fix)
RUN apk --no-cache add patch

# 1) Deps layer — cached until go.mod/go.sum change
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# 2) Vendor + patch layer — cached until patches/ or go.mod change
COPY patches/ patches/
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod vendor && \
    patch -p1 < patches/gocql-nil-conn-guard.patch

# 3) Source layer — only rebuilds when .go files change
COPY . .

# Static binary: CGO_ENABLED=0 → no libc, runs on scratch/alpine/distroless
# -mod=vendor uses patched gocql with nil-Conn guard for ScyllaDB Cloud
# -trimpath removes local paths from binary (reproducible builds, no info leak)
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -mod=vendor -trimpath \
    -ldflags="-s -w -X scylla2ch/cmd.Version=${VERSION}" \
    -o /scylla2ch .

# ── Run stage ────────────────────────────────────────────────────────
# alpine:3.20 = 5MB base — gives us wget (healthcheck), sh (debug), ca-certs
FROM alpine:3.20
RUN apk --no-cache add ca-certificates tzdata

# Non-root user
RUN addgroup -S scylla2ch && adduser -S -G scylla2ch scylla2ch

# Config directory for volume mounts + data directory for checkpoint + DLQ
RUN mkdir -p /etc/scylla2ch/certs /etc/scylla2ch/tables.d /var/lib/scylla2ch/dlq && \
    chown -R scylla2ch:scylla2ch /etc/scylla2ch /var/lib/scylla2ch

COPY --from=builder /scylla2ch /usr/local/bin/scylla2ch

USER scylla2ch

# Prometheus metrics + health check
EXPOSE 9090 8080

ENTRYPOINT ["scylla2ch"]
CMD ["run", "--config", "/etc/scylla2ch/config.yaml"]
