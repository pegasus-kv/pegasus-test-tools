default: build

build:
	go build -o bin/toolbox github.com/pegasus-kv/pegasus-test-tools/cmd/toolbox
	cp config-dcheck.json bin/
	cp config-scheck.json bin/

format:
	gofmt -w cmd/ tools
