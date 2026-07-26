# Multi-stage build for Cogneva (Rust)
# Compatible with Docker, buildah, podman, and containerd (K3s)

# ------------------------------------------------------------------------------
# Stage 1: Build
# ------------------------------------------------------------------------------
FROM rust:1.85-slim AS builder

WORKDIR /build

# Install build dependencies (libssl-dev for rustls-tls, pkg-config for sqlx)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade Rust toolchain to match the workspace's actual dependency requirements.
# The base image ships 1.85, but Cargo.lock contains crates requiring 1.91+.
RUN rustup toolchain install 1.95.0 && rustup default 1.95.0

# Copy workspace manifest files first for dependency caching
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY crates/cog-core/Cargo.toml crates/cog-core/
COPY crates/cog-llm/Cargo.toml crates/cog-llm/
COPY crates/cog-agent/Cargo.toml crates/cog-agent/
COPY crates/cog-collaboration/Cargo.toml crates/cog-collaboration/
COPY crates/cog-orchestrator/Cargo.toml crates/cog-orchestrator/
COPY crates/cog-gateway/Cargo.toml crates/cog-gateway/
COPY crates/cog-auth/Cargo.toml crates/cog-auth/
COPY crates/cog-quota/Cargo.toml crates/cog-quota/
COPY crates/cog-supervisor/Cargo.toml crates/cog-supervisor/
COPY crates/cog-wiki/Cargo.toml crates/cog-wiki/
COPY crates/cog-observability/Cargo.toml crates/cog-observability/
COPY crates/cog-storage/Cargo.toml crates/cog-storage/
COPY crates/cog-memory/Cargo.toml crates/cog-memory/
COPY crates/cogneva/Cargo.toml crates/cogneva/

# Create dummy main.rs files to build and cache dependencies
RUN mkdir -p crates/cog-core/src crates/cog-llm/src crates/cog-agent/src \
    crates/cog-collaboration/src crates/cog-orchestrator/src crates/cog-gateway/src \
    crates/cog-auth/src crates/cog-quota/src crates/cog-supervisor/src \
    crates/cog-wiki/src crates/cog-observability/src crates/cog-storage/src \
    crates/cog-memory/src crates/cogneva/src

RUN for crate in cog-core cog-llm cog-agent cog-collaboration cog-orchestrator cog-gateway cog-auth cog-quota cog-supervisor cog-wiki cog-observability cog-storage cog-memory cogneva; do \
    echo 'fn main() {}' > crates/$crate/src/main.rs 2>/dev/null || true; \
    done

# Build dependencies (cached layer)
RUN cargo build --release --locked --bin cogneva 2>/dev/null || true

# Copy actual source code
COPY crates/ ./crates/

# Touch source files to invalidate cached object files only
RUN touch crates/*/src/lib.rs crates/*/src/main.rs 2>/dev/null || true

# Build the release binary
RUN cargo build --release --locked --bin cogneva

# Strip the binary to reduce size
RUN strip target/release/cogneva

# ------------------------------------------------------------------------------
# Stage 2: Runtime
# ------------------------------------------------------------------------------
FROM ubuntu:24.04

# Install runtime dependencies (ca-certificates for TLS, libssl3 for rustls)
# and Rust toolchain so the self-evolution worker can build patches inside K3s.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    curl \
    build-essential \
    git \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Install rustup/stable toolchain for the self-evolution worker.
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable \
    && rustup component add rustfmt clippy

# Create FHS directory structure
RUN mkdir -p /opt/cogneva /var/lib/cogneva-data /etc/cogneva /run/cogneva

# Create non-root user (overridden to root in evolution mode for build tooling)
RUN groupadd -r cogneva -g 1001 && \
    useradd -r -g cogneva -u 1001 -d /opt/cogneva -s /sbin/nologin cogneva

# Copy the built binary
COPY --from=builder /build/target/release/cogneva /opt/cogneva/cogneva

# Copy SQL migrations so the storage plugin can apply them at runtime
COPY --from=builder /build/crates/cog-storage/migrations /opt/cogneva/crates/cog-storage/migrations

# Set ownership
RUN chown -R cogneva:cogneva /opt/cogneva /var/lib/cogneva-data /etc/cogneva && \
    chmod +x /opt/cogneva/cogneva

# Default environment variables (overridden by K8s ConfigMap/Secret)
ENV SF_APP_NAME=cogneva \
    SF_APP_VERSION=0.1.20 \
    SF_LOG_LEVEL=info \
    SF_DATA_DIR=/var/lib/cogneva-data \
    SF_CONFIG_DIR=/etc/cogneva \
    SF_APP_DIR=/opt/cogneva \
    SF_DB_PROVIDER=mysql \
    SF_PG_PROVIDER=postgres \
    SF_VECTOR_PROVIDER=lancedb \
    SF_MEDIA_PROVIDER=local-sfu \
    SF_STORAGE_PROVIDER=local-fs \
    SF_REDIS_URL=redis://127.0.0.1:6379 \
    SF_WORKSPACE_ID=default \
    SF_CONSUMER_GROUP=cogneva \
    SF_HTTP_PORT=8080 \
    SF_WS_PORT=8081 \
    SF_METRICS_PORT=9090 \
    RUST_LOG=info

WORKDIR /opt/cogneva

EXPOSE 8080 8081 9090

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD ["/opt/cogneva/cogneva", "--health-check"] || exit 1

ENTRYPOINT ["/opt/cogneva/cogneva"]
