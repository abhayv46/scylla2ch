# scylla2ch

Direct ScyllaDB CDC to ClickHouse replicator. No Kafka, no Debezium, no middleware.

```
ScyllaDB CDC log  ──→  scylla2ch  ──→  ClickHouse
  (source of truth)    (Go binary)      (analytics)
```

## What it does

Reads Change Data Capture events directly from ScyllaDB CDC log tables and writes them to ClickHouse using batched inserts. One binary, ~4,600 lines of Go.

**Key features:**
- **Zero middleware** — reads CDC logs directly (no Kafka/Debezium/Zookeeper)
- **Channel-per-table** — each table gets its own buffered channel + writer goroutine. A slow flush to table A never blocks table B.
- **Auto-discover** — detects CDC-enabled tables automatically from `system_schema`. No config per table.
- **Auto-create** — generates ClickHouse `ReplacingMergeTree` DDL from ScyllaDB schema.
- **Hot-reload** — new CDC tables detected and streamed without restart (30s scan interval).
- **SIGHUP config reload** — add tables without restart (`kill -HUP`, `docker kill --signal=HUP`, `systemctl reload`).
- **Drop-in table configs** — `tables.d/*.yaml` (like supervisor `conf.d/`) — drop a file, send SIGHUP, done.
- **Multi-target routing** — split one ScyllaDB table into N ClickHouse tables using filter expressions.
- **Snapshot** — bulk-load existing data before starting CDC streaming.
- **TLS security warnings** — detects plaintext credentials for non-localhost connections.
- **20 Prometheus metrics** — throughput, latency, buffer depth, lag, retries, drops, connection health, reloads.
- **Graceful shutdown** — SIGTERM drains all buffers before exiting.

## Quick start

### 1. Prerequisites

- Go 1.26+
- ScyllaDB with CDC enabled on target tables
- ClickHouse (Cloud or self-hosted)

### 2. Build

```bash
git clone <repo> && cd scylla2ch
make build
```

### 3. Configure

```bash
cp .env.example .env
# Edit .env with your ScyllaDB + ClickHouse credentials

cp config/config.yaml.example config/config.yaml
# Edit config.yaml (all values support ${ENV_VAR} expansion)
```

### 4. Validate connectivity

```bash
./scylla2ch validate --config config/config.yaml
```

### 5. Snapshot existing data (optional, one-time)

```bash
./scylla2ch snapshot --config config/config.yaml --table rooms
```

### 6. Start CDC streaming

```bash
./scylla2ch run --config config/config.yaml
```

## Docker

```bash
# Build image (BuildKit cached — rebuild in ~3s on code change)
make docker

# Run with docker-compose
docker compose up -d

# Check health
curl http://localhost:8080/

# Check metrics
curl http://localhost:9090/metrics

# SIGHUP reload — add tables without restart
docker kill --signal=HUP scylla2ch

# Validate connectivity
docker exec scylla2ch scylla2ch validate --config /etc/scylla2ch/config.yaml
```

## Commands

| Command | Description |
|---------|-------------|
| `run` | Start CDC streaming (main command) |
| `snapshot --table <name>` | Bulk-load existing table data to ClickHouse |
| `validate` | Test ScyllaDB + ClickHouse connectivity |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              ScyllaDB CDC Log Tables                 │
│  rooms_scylla_cdc_log  │  messages_scylla_cdc_log   │
└──────────┬─────────────┴──────────────┬──────────────┘
           │                            │
     CDC Consumer              CDC Consumer
     (goroutine)               (goroutine)
           │                            │
     ┌─────▼─────┐              ┌───────▼───────┐
     │ rooms ch  │              │ messages ch   │
     │ (buf 20K) │              │ (buf 20K)     │
     └─────┬─────┘              └───────┬───────┘
           │                            │
     tableWriter               tableWriter
     (goroutine)               (goroutine)
           │                            │
     batch flush               batch flush
     (10K or 5s)               (10K or 5s)
           │                            │
     ┌─────▼────────────────────────────▼──────┐
     │            ClickHouse                    │
     │  ReplacingMergeTree(_cdc_ts)             │
     │  Last-write-wins deduplication           │
     └─────────────────────────────────────────┘
```

Each table is completely independent. Adding a new table has zero impact on existing ones.

## Project structure

```
scylla2ch/
├── main.go                 # Entry point
├── cmd/
│   ├── root.go             # CLI framework (cobra)
│   ├── run.go              # CDC streaming command
│   ├── snapshot.go          # Bulk-load command
│   └── validate.go          # Connectivity test
├── config/
│   ├── config.go            # YAML loader + env expansion + validation + tables_dir
│   ├── config.yaml.example  # Example config with comments
│   └── tables.d/            # Drop-in table configs (each *.yaml = one table)
├── consumer/
│   ├── consumer.go          # CDC event processor + type converter
│   └── factory.go           # scylla-cdc-go ChangeConsumerFactory
├── converter/
│   ├── schema.go            # Schema introspection + DDL generation
│   └── types.go             # CQL → ClickHouse type mapping
├── writer/
│   └── clickhouse.go        # Channel-per-table batch writer
├── metrics/
│   └── prometheus.go        # 20 Prometheus metrics + HTTP server
├── docs/
│   ├── architecture.md      # Full architecture docs + comparisons
│   └── operations-guide.md  # Production operations guide
├── Dockerfile               # Multi-stage build (alpine)
├── docker-compose.yml       # Production-ready compose
├── scylla2ch.service        # systemd unit file
├── Makefile                 # Build/test/lint/docker commands
├── .env.example             # Environment variable template
└── .gitignore
```

## Comparison

| Feature | scylla2ch | Debezium + Kafka | PeerDB | Custom Python |
|---------|-----------|------------------|--------|---------------|
| Moving parts | 1 binary | 4+ (ZK, Kafka, Connect, Debezium) | SaaS / 3+ containers | 1 script |
| Latency | ~5s (CDC confidence) | ~10-30s | ~10s | ~30-60s |
| Throughput | 50K+ rows/sec | 20-50K rows/sec | 30K+ rows/sec | 1-5K rows/sec |
| Memory | 50-200 MB | 2-4 GB | 500 MB-1 GB | 100-500 MB |
| ScyllaDB CDC native | Yes | No (uses Cassandra connector) | No | Manual |
| Auto-discover | Yes | No | No | No |
| Hot-reload | Yes | Restart required | Restart required | Restart required |

## Monitoring

Prometheus metrics at `:9090/metrics`:

- `scylla2ch_rows_consumed_total` — CDC rows read
- `scylla2ch_rows_flushed_total` — rows written to ClickHouse
- `scylla2ch_rows_dropped_total` — rows lost after retry exhaustion (ALERT on > 0)
- `scylla2ch_cdc_lag_seconds` — replication lag
- `scylla2ch_buffer_rows` — channel buffer depth (back-pressure indicator)
- `scylla2ch_flush_duration_seconds` — flush latency histogram

Health check at `:8080/`.

## Deployment options

1. **Docker** — `docker compose up -d` (recommended)
2. **Kubernetes** — use the Dockerfile, mount config via ConfigMap, secrets via K8s Secrets
3. **systemd** — copy `scylla2ch.service` to `/etc/systemd/system/`, binary to `/usr/local/bin/`

See [docs/architecture.md](docs/architecture.md) for the complete guide: architecture, delivery guarantees, ClickHouse table structure, deployment, operations, monitoring, and troubleshooting.

## License

Internal use.
