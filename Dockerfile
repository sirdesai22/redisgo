# Step 1: Build binary inside Go container
FROM golang:1.25 AS builder

WORKDIR /app
COPY . .

# Download dependencies (optional but good for caching)
RUN go mod download

# Build RedisGo binary
RUN go build -o redisgo main.go

# Step 2: Run lightweight container
FROM debian:bookworm-slim

# Add non-root user (good practice)
RUN useradd -m redisgo

# Copy binary from builder stage
COPY --from=builder /app/redisgo /usr/local/bin/

# Switch to non-root
USER redisgo

# Expose Redis default port
EXPOSE 6379

# Run RedisGo
CMD ["redisgo"]
