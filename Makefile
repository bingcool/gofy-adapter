GO      ?= go
GOFLAGS ?=
PKGS    := ./...

.PHONY: fmt fmt-check vet test test-race guards check

fmt:
	gofmt -l -w .

fmt-check:
	@files="$$(gofmt -l .)"; \
	if [ -n "$$files" ]; then echo "gofmt required:"; echo "$$files"; exit 1; fi

vet:
	$(GO) vet $(GOFLAGS) $(PKGS)

test:
	$(GO) test $(GOFLAGS) -count=1 $(PKGS)

test-race:
	$(GO) test $(GOFLAGS) -race -count=1 $(PKGS)

guards:
	./shell/check-imports.sh

check: fmt-check vet test-race guards
