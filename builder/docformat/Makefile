CURDIR:=$(shell pwd)
APP:=docformat
all:
	gofmt -w src/main src/job src/common
	export GOPATH=$(CURDIR) && go build -o bin/$(APP) main

clean:
	rm -f bin/$(APP)

test:
	export GOPATH=$(CURDIR) && cd src/job && go test -v

benchmark:
	export GOPATH=$(CURDIR) && cd src/job && go test -test.bench=".*"

.PHONY:all clean test benchmark
