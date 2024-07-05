.DEFAULT_GOAL := help

# Looks at comments using ## on targets and uses them to produce a help output.
.PHONY: help
help: ALIGN=14
help: ## Print this message
	@awk -F ': .*## ' -- "/^[^':]+: .*## /"' { printf "'$$(tput bold)'%-$(ALIGN)s'$$(tput sgr0)' %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: fmt
fmt: ## Autoformat code with Rye/Ruff
	rye fmt

.PHONY: generate
generate: ## Produce all generated artifacts
generate: generate/sqlc

.PHONY: generate/sqlc
generate/sqlc: ## Generate sqlc code
	cd src/riverqueue/driver/riversqlalchemy/dbsqlc && sqlc generate

.PHONY: lint
lint: ## Run linter with Rye/Ruff
	rye lint

.PHONY: test
test: ## Run test suite with Rye/pytest
	rye test

.PHONY: type-check
type-check: ## Run type check with MyPy
	rye run mypy -p riverqueue -p examples -p tests

.PHONY: verify
verify: ## Verify all generated artifacts
verify: verify/sqlc

.PHONY: verify/sqlc
verify/sqlc: # Verify sqlc code
	cd src/riverqueue/driver/riversqlalchemy/dbsqlc && sqlc verify