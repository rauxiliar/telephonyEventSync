# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies with security updates
RUN apk add --no-cache --upgrade git

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download and verify dependencies
RUN go mod download && \
    go mod tidy && \
    go mod verify

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o telephonyEventSync ./src

# Final stage
FROM alpine:3.21

WORKDIR /app

# Install runtime dependencies with security updates
RUN apk add --no-cache --upgrade ca-certificates tzdata

# Copy binary
COPY --from=builder /app/telephonyEventSync .

# Create non-root user
RUN adduser -D -g '' appuser && \
    chown -R appuser:appuser /app
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run the application
CMD ["./telephonyEventSync"]
