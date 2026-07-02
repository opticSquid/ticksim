.PHONY: proto clean help

##proto-check: Run Buf compiler to check Protobuf definitions
proto-check:
	@echo "Running Buf compiler to check for errors in proto files"
	@buf build
	@echo "No errors found"

## proto: Generate Go files from Protobuf definitions using Buf
proto-gen:
	@echo "Generating Protobuf code via Buf..."
	@buf generate

## clean: Remove all generated .pb.go files safely
clean:
	@echo "Cleaning up generated files..."
	@find api/ -type f -name "*.pb.go" -delete

## help: Show available commands
help:
	@echo "Available commands:"
	@sed -n 's/^##//p' Makefile | column -t -s ':' |  sed -e 's/^/ /'
