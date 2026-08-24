# Frankenqueue. Everything a reviewer needs is a single make target.

.DEFAULT_GOAL := help
.PHONY: help run demo verify test race bench fmt clean

help: ## Show these targets
	@echo
	@echo "  Frankenqueue"
	@echo
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-9s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  Start here:  make demo     a full self contained tour, nothing to set up"
	@echo "               make verify   one command, one verdict, real exit code"
	@echo

demo: ## Full automated tour: server, every feature, SIGKILL durability proof
	@./scripts/demo.sh

verify: ## Build, gofmt, vet, race suite, live durability proof. Exits non zero on failure
	@./scripts/verify.sh

run: ## Start the server on :8080 with a durable log at ./data
	go run .

test: ## Run the test suite
	go test ./...

race: ## Run the suite under the race detector, the concurrency evidence
	go test -race -count=1 ./...

bench: ## Benchmark the cost of durability at each fsync setting
	go test -bench=. -benchtime=500x -run=XXX ./queue/

fmt: ## Format everything
	gofmt -w .

clean: ## Delete the durable log. Stop the server first
	rm -rf data
