BINARY_NAME=rt_stats

.PHONY: build run test clean

build:
	go build -o $(BINARY_NAME) ./cmd/gw

run: build
	./$(BINARY_NAME)

test:
	go test -v -race ./...

clean:
	go clean
	rm -f $(BINARY_NAME)
	rm -f /tmp/rt_stats_snapshot.json