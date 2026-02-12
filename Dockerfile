# Build Stage
FROM rustlang/rust:nightly-bookworm-slim AS builder

WORKDIR /usr/src/app

# Install system dependencies
# Install system dependencies
RUN apt-get update && apt-get install -y cmake musl-tools pkg-config libssl-dev prometheus-cpp-dev build-essential

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy src to build dependencies
RUN mkdir -p src/bin
RUN echo "fn main() {}" > src/main.rs
RUN echo "fn main() {}" > src/bin/load_tester.rs

# Build dependencies
RUN cargo build --release
RUN rm -rf src

# Copy source code
COPY src ./src
COPY .env ./.env

# Build application
RUN touch src/main.rs
RUN cargo build --release

# Runtime Stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libssl3 ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/bin

COPY --from=builder /usr/src/app/target/release/poc-mqtt-backend .
COPY --from=builder /usr/src/app/.env .

# Expose Metrics Port
EXPOSE 9000

CMD ["./poc-mqtt-backend"]
