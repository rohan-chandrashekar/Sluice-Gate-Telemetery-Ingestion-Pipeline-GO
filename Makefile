SHELL := /bin/bash
export PATH := $(HOME)/sdk/go/bin:$(HOME)/go/bin:$(HOME)/.local/bin:$(PATH)

.PHONY: proto build test fmt vet run-gateway run-consumer loadgen report \
	phase0 phase1 phase2 phase3 phase4 phase5 \
	helm-template terraform-validate clean

proto:
	./scripts/gen_proto.sh

build:
	mkdir -p bin
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/consumer ./cmd/consumer
	go build -o bin/loadgen ./cmd/loadgen
	go build -o bin/report ./cmd/report
	go build -o bin/topicinit ./cmd/topicinit
	go build -o bin/losstest ./cmd/losstest
	go build -o bin/injectmalformed ./cmd/injectmalformed
	go build -o bin/topiccount ./cmd/topiccount

test:
	go test ./...

fmt:
	gofmt -l .

vet:
	go vet ./...

run-gateway:
	go run ./cmd/gateway $(ARGS)

run-consumer:
	go run ./cmd/consumer $(ARGS)

loadgen:
	go run ./cmd/loadgen $(ARGS)

report:
	go run ./cmd/report $(ARGS)

phase0:
	./scripts/run_phase0.sh

phase1:
	./scripts/run_phase1.sh

phase2:
	./scripts/run_phase2.sh

phase3:
	./scripts/run_phase3.sh

phase4:
	./scripts/run_phase4.sh

phase5:
	./scripts/run_phase5.sh

helm-template:
	helm template sluice deploy/helm/sluice

terraform-validate:
	cd deploy/terraform && terraform init -backend=false && terraform validate

clean:
	rm -rf bin/
