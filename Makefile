BINARY  := scylla2ch
VERSION := $(shell git describe --tags --always 2>/dev/null || echo "1.0.0")
GOFLAGS := -ldflags="-s -w -X scylla2ch/cmd.Version=$(VERSION)"

.PHONY: build test lint run snapshot validate clean docker docker-run docker-stop docker-reload docker-logs docker-sh vendor help

## Build
build:                     ## Build the binary (static, no CGO)
	CGO_ENABLED=0 go build -trimpath $(GOFLAGS) -o $(BINARY) .

## Test
test:                      ## Run all tests with race detector
	go test ./... -race -count=1 -timeout=120s

test-v:                    ## Run tests with verbose output
	go test ./... -race -count=1 -timeout=120s -v

## Lint
lint:                      ## Run go vet + staticcheck
	go vet ./...
	staticcheck ./...

## Run commands
run:                       ## Run CDC streaming (requires config.yaml)
	go run . run --config config/config.yaml.example

snapshot:                  ## Snapshot a table (TABLE=rooms make snapshot)
	go run . snapshot --config config/config.yaml.example --table $(TABLE)

validate:                  ## Validate config + connectivity
	go run . validate --config config/config.yaml.example

## Docker — build
docker:                    ## Build Docker image (BuildKit cached)
	DOCKER_BUILDKIT=1 docker build \
		--build-arg VERSION=$(VERSION) \
		-t $(BINARY):$(VERSION) \
		-t $(BINARY):latest .

## Docker — run
docker-run:                ## Start with docker compose (detached)
	VERSION=$(VERSION) docker compose up -d

docker-stop:               ## Stop docker compose
	docker compose down

docker-rebuild:            ## Rebuild + restart (zero downtime pattern)
	DOCKER_BUILDKIT=1 docker compose build --build-arg VERSION=$(VERSION)
	docker compose up -d

## Docker — operate
docker-reload:             ## SIGHUP reload — pick up new tables without restart
	docker kill --signal=HUP scylla2ch

docker-logs:               ## Tail container logs
	docker compose logs -f --tail=100 scylla2ch

docker-status:             ## Show container status + resource usage
	@docker compose ps
	@echo "---"
	@docker stats --no-stream scylla2ch 2>/dev/null || true

## Vendor (run after go mod tidy)
vendor:                    ## Vendor deps + apply gocql nil-Conn patch
	go mod vendor
	@echo "Applying gocql nil-Conn guard patch..."
	patch -p1 < patches/gocql-nil-conn-guard.patch || true
	@echo "Vendor complete. Patch applied."

## Clean
clean:                     ## Remove binary
	rm -f $(BINARY)

## Help
help:                      ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
