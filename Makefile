include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: all test build clean
SHELL := /bin/bash
PKGS = $(shell GO15VENDOREXPERIMENT=1 go list ./... | grep -v "vendor/" | grep -v "db")
$(eval $(call golang-version-check,1.8))

$(GOPATH)/bin/glide:
	@go get github.com/Masterminds/glide


test: $(PKGS)
$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)

build:
# disable CGO and link completely statically (this is to enable us to run in containers that don't use glibc)
	@CGO_ENABLED=0 go build -a -installsuffix cgo

install_deps: $(GOPATH)/bin/glide
	@$(GOPATH)/bin/glide install

run: build
	./fluentd-kinesis-forwarder-monitor
