# Stage 1: Build binary using cargo chef or standard cache mounts
FROM rust:slim-bookworm AS builder

WORKDIR /usr/src/s3xc

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies by building a dummy main first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# Copy real source code and build the application
COPY src ./src
# Touch main.rs to force rebuild
RUN touch src/main.rs
RUN cargo build --release

# Stage 2: Distroless minimal runtime image
FROM debian:bookworm-slim

# Install ca-certificates (needed for SSL connections to upstream S3 backends)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/s3xc/target/release/s3xc /usr/local/bin/s3xc

# Create cache directory
RUN mkdir -p /var/cache/s3xc

# Expose HTTP port
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/s3xc"]
