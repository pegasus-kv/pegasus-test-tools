default: build

build:
	go build -o bin/toolbox github.com/pegasus-kv/pegasus-test-tools/cmd/toolbox

format:
	gofmt -w cmd/ tools
