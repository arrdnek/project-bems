# Build stage for Go application
FROM golang:1.23.0 AS builder

WORKDIR /app

# Copy go.mod and go.sum for dependency installation
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build the application
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -o main .

# Runtime stage
FROM debian:bullseye-slim

WORKDIR /app

# Copy the binary application and startup script from the builder
COPY --from=builder /app/main .
COPY start.sh .

# Make sure the binary and script are executable
RUN chmod +x main
RUN chmod +x start.sh

# Install MariaDB client tools
RUN apt-get update && apt-get install -y mariadb-client && rm -rf /var/lib/apt/lists/*

# Expose the application port
EXPOSE 10005

# Run the application
CMD ["./start.sh"]