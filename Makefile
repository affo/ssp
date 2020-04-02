generate:
	go generate ./graph
	go generate ./naive
	go generate ./values
	go generate ./

test:
	go test ./...

tidy: fmt lint
	go mod tidy

fmt:
	go fmt ./...

lint:
	golangci-lint run

