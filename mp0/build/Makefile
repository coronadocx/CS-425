.PHONY: all
all: central_logger node

central_logger: 
	go build ../src/central_logger.go

node:
	go build ../src/node.go

clean:
	rm -f node central_logger