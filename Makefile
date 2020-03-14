generate:
	go generate ./graph
	go generate ./

test:
	go test ./...

mod:
	go mod tidy

fmt:
	go fmt ./...
