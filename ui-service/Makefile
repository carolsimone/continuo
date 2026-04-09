.PHONY: build dev install test lint fmt clean

# Build Docker image
build:
	docker build -t continuo-ui:dev .

# Run in development mode (Vite + Express, requires npm install first)
dev:
	npm run dev

# Install dependencies
install:
	npm ci

# Run tests
test:
	npm test

# Run linter
lint:
	npm run lint

# Format code
fmt:
	npm run fmt

# Clean build artifacts
clean:
	rm -rf dist/ dist-server/ node_modules/
