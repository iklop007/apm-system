BIN_DIR := bin

.PHONY: build test run run-demo clean

build:
	go build -o $(BIN_DIR)/apm-server ./cmd/apm-server
	go build -o $(BIN_DIR)/demo-app ./cmd/demo-app

test:
	go test ./...

run: build
	./$(BIN_DIR)/apm-server

run-demo: build
	./$(BIN_DIR)/demo-app

clean:
	rm -rf $(BIN_DIR) data
