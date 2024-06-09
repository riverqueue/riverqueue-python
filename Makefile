.PHONY: fmt
fmt:
	rye fmt

.PHONY: lint
lint:
	rye lint

.PHONY: test
test:
	rye test

.PHONY: typecheck
typecheck:
	rye run mypy -p src.riverqueue
