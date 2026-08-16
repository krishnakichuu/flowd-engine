SHELL := /bin/bash

DB_URL ?= postgres://flowd:flowd@localhost:5432/flowd?sslmode=disable

# The Java SDK (../sdk-java) is a standalone sibling Maven project now, not
# part of this build graph — it owns its own copy of api/proto and
# generates its own stubs at build time (see ../sdk-java/pom.xml). Build it
# from there: cd ../sdk-java && mvn test.

.PHONY: proto-gen sqlc-gen generate lint test test-unit test-integration \
        migrate-up migrate-down web-build build run-server run-worker compose-up compose-down verify

generate: proto-gen sqlc-gen

proto-gen:
	buf generate

sqlc-gen:
	sqlc generate

lint:
	golangci-lint run ./...
	buf lint
	buf breaking --against '.git#branch=main'

test: test-unit

test-unit:
	go test -short ./...
	go test -short ./sdk/...

test-integration:
	go test -tags=integration ./test/...

migrate-up:
	migrate -database "$(DB_URL)" -path migrations up

migrate-down:
	migrate -database "$(DB_URL)" -path migrations down 1

# Refreshes internal/webui's embedded copy from a real frontend build.
# internal/webui/dist is committed with the most recent build so `go build`
# alone (without ever running this) still produces a working binary — this
# target is what keeps that embedded copy from going stale.
web-build:
	cd web && npm install && npm run build
	rm -rf internal/webui/dist
	cp -r web/dist internal/webui/dist

build: web-build
	go build -o bin/flowd ./cmd/flowd
	go build -o bin/flow-cli ./cmd/flow-cli

run-server: build
	./bin/flowd

compose-up:
	docker compose up -d postgres

compose-down:
	docker compose down

# Full local verification: bring up postgres, migrate, run unit + integration tests.
verify: compose-up
	@until docker compose exec -T postgres pg_isready -U flowd -d flowd >/dev/null 2>&1; do sleep 1; done
	$(MAKE) migrate-up
	$(MAKE) test-unit
	$(MAKE) test-integration
