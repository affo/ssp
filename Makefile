generate:
	go generate ./graph
	go generate ./naive
	go generate ./values
	go generate ./

test:
	go test ./...

mod:
	go mod tidy

fmt:
	go fmt ./...
