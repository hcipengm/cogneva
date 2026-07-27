# Multi-stage build for Cogneva (Rust)
# Compatible with Docker, buildah, podman, and containerd (K3s)

# 受限网络（CN）构建参数，默认空 = 官方源；引导器在 CN_MIRROR 模式下传入 TUNA 镜像。
# RUST_TOOLCHAIN：官方源按版本钉死；TUNA 不镜像按版本 channel（仅 stable/beta/nightly），
# CN 模式必须传 stable。
ARG RUST_TOOLCHAIN="1.95.0"
ARG RUSTUP_DIST_SERVER=""
ARG RUSTUP_UPDATE_ROOT=""
ARG RUSTUP_INIT_URL="https://sh.rustup.rs"
ARG CARGO_REGISTRY_SPARSE=""
# CN 模式传镜像主机（如 mirrors.tuna.tsinghua.edu.cn），替换 debian/ubuntu 官方源
ARG APT_MIRROR_HOST=""
# 低内存机器（如 2-4G 空白机）限制 cargo 并行度防 OOM；"default" = 按核数自动
#（不能放空字符串，cargo 会解析报错）
ARG CARGO_BUILD_JOBS="default"

# ------------------------------------------------------------------------------
# Stage 1: Build
# ------------------------------------------------------------------------------
# 基底必须 ubuntu 24.04（glibc 2.39），不能用 debian bookworm（glibc 2.36）：
# fastembed/ort 下载的 ONNX Runtime 预编译库引用 __isoc23_*（glibc 2.38+）符号，
# bookworm 上最终链接必挂（undefined symbol: __isoc23_strtoll）。
FROM ubuntu:24.04 AS builder

ARG RUST_TOOLCHAIN
ARG RUSTUP_DIST_SERVER
ARG RUSTUP_UPDATE_ROOT
ARG RUSTUP_INIT_URL
ARG CARGO_REGISTRY_SPARSE
ARG APT_MIRROR_HOST
ARG CARGO_BUILD_JOBS
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS} \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

WORKDIR /build

# 受限网络：ubuntu 源替换为镜像主机（deb822 与旧格式都覆盖）
RUN if [ -n "$APT_MIRROR_HOST" ]; then \
    sed -i "s|archive.ubuntu.com|$APT_MIRROR_HOST|g; s|security.ubuntu.com|$APT_MIRROR_HOST|g" \
    /etc/apt/sources.list /etc/apt/sources.list.d/*.sources 2>/dev/null || true; \
    fi

# Install build dependencies (libssl-dev for rustls-tls, pkg-config for sqlx,
# protobuf-compiler for etcd-client, build-essential 提供 cc/g++/libstdc++.so
# —— ort 的 ONNX Runtime C++ 库最终链接需要）
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    ca-certificates \
    curl \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Install the Rust toolchain via rustup (ubuntu 基底不自带 Rust)。
# Cargo.lock contains crates requiring 1.91+, 按 RUST_TOOLCHAIN 钉版本。
# 注意 TUNA 不托管 rustup-init.sh 脚本（404），CN 模式直接拉
# $RUSTUP_UPDATE_ROOT/dist/<arch>-unknown-linux-gnu/rustup-init 二进制。
RUN if [ -n "$RUSTUP_DIST_SERVER" ]; then \
    export RUSTUP_DIST_SERVER RUSTUP_UPDATE_ROOT; \
    arch=$(uname -m); \
    curl --proto '=https' --tlsv1.2 -fsSL "$RUSTUP_UPDATE_ROOT/dist/$arch-unknown-linux-gnu/rustup-init" -o /tmp/rustup-init && \
    chmod +x /tmp/rustup-init && /tmp/rustup-init -y --default-toolchain none; \
    else \
    curl --proto '=https' --tlsv1.2 -sSf "$RUSTUP_INIT_URL" | sh -s -- -y --default-toolchain none; \
    fi && \
    /usr/local/cargo/bin/rustup toolchain install "$RUST_TOOLCHAIN" && \
    /usr/local/cargo/bin/rustup default "$RUST_TOOLCHAIN"

# 受限网络：crates 走 sparse 镜像。注意 TUNA 的 crates 镜像只镜像索引、
# crate 文件回源 static.crates.io（其 config.json 的 dl 字段），国内必挂；
# 必须用 rsproxy（字节 CDN）或 USTC 这类索引+文件都自托管的镜像。
# multiplexing=false 强制 HTTP/1.1，规避 HTTP/2 被中间设备干扰（PROTOCOL_ERROR 重置）
RUN if [ -n "$CARGO_REGISTRY_SPARSE" ]; then \
    mkdir -p /usr/local/cargo && \
    printf '[source.crates-io]\nreplace-with = "mirror"\n[source.mirror]\nregistry = "sparse+%s"\n\n[http]\nmultiplexing = false\n\n[net]\nretry = 10\n' "$CARGO_REGISTRY_SPARSE" > /usr/local/cargo/config.toml; \
    fi

# Copy workspace manifest files first for dependency caching.
# 工作区全部 26 个成员的 Cargo.toml 都必须齐全，缺任何一个 workspace 解析直接失败
#（曾经只复制 14 个，导致下面的依赖预热层秒挂、缓存从未生效）。
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY crates/cog-core/Cargo.toml crates/cog-core/
COPY crates/cog-storage/Cargo.toml crates/cog-storage/
COPY crates/cog-llm/Cargo.toml crates/cog-llm/
COPY crates/cog-agent/Cargo.toml crates/cog-agent/
COPY crates/cog-collaboration/Cargo.toml crates/cog-collaboration/
COPY crates/cog-orchestrator/Cargo.toml crates/cog-orchestrator/
COPY crates/cogneva/Cargo.toml crates/cogneva/
COPY crates/cog-auth/Cargo.toml crates/cog-auth/
COPY crates/cog-quota/Cargo.toml crates/cog-quota/
COPY crates/cog-wiki/Cargo.toml crates/cog-wiki/
COPY crates/cog-observability/Cargo.toml crates/cog-observability/
COPY crates/cog-memory/Cargo.toml crates/cog-memory/
COPY crates/cog-reflection/Cargo.toml crates/cog-reflection/
COPY crates/cog-prompt/Cargo.toml crates/cog-prompt/
COPY crates/cog-eval/Cargo.toml crates/cog-eval/
COPY crates/cog-guardrail/Cargo.toml crates/cog-guardrail/
COPY crates/cog-protocol/Cargo.toml crates/cog-protocol/
COPY crates/cog-stream/Cargo.toml crates/cog-stream/
COPY crates/cog-net/Cargo.toml crates/cog-net/
COPY crates/cog-notification/Cargo.toml crates/cog-notification/
COPY crates/cog-extension/Cargo.toml crates/cog-extension/
COPY crates/cog-skill/Cargo.toml crates/cog-skill/
COPY crates/cog-gateway/Cargo.toml crates/cog-gateway/
COPY crates/cog-supervisor/Cargo.toml crates/cog-supervisor/
COPY crates/cog-github/Cargo.toml crates/cog-github/
COPY crates/bootstrap/Cargo.toml crates/bootstrap/

# Create dummy sources so the dependency warm-up layer compiles.
# lib 型 crate 必须补空 lib.rs；Cargo.toml 里显式声明的 target
#（cog-storage 的 cog-migrate bin、cog-collaboration 的 pge_cycle bench）也要补占位文件。
RUN for crate in cog-core cog-storage cog-llm cog-agent cog-collaboration cog-orchestrator \
    cogneva cog-auth cog-quota cog-wiki cog-observability cog-memory cog-reflection cog-prompt \
    cog-eval cog-guardrail cog-protocol cog-stream cog-net cog-notification cog-extension \
    cog-skill cog-gateway cog-supervisor cog-github bootstrap; do \
    mkdir -p crates/$crate/src && \
    echo '' > crates/$crate/src/lib.rs && \
    echo 'fn main() {}' > crates/$crate/src/main.rs; \
    done && \
    mkdir -p crates/cog-storage/src/migrate crates/cog-collaboration/benches && \
    echo 'fn main() {}' > crates/cog-storage/src/migrate/main.rs && \
    echo 'fn main() {}' > crates/cog-collaboration/benches/pge_cycle.rs

# Build and cache all third-party dependencies (strict: 失败必须暴露，不允许静默退化为无缓存)。
# fetch 单独加 shell 重试：镜像偶发单 crate 失败时，重试在同一层内进行、
# 已下载的 registry 缓存不会丢（层失败被 buildah 整体丢弃的教训）。
RUN ok=0; for i in $(seq 1 10); do cargo fetch --locked && { ok=1; break; }; \
    echo "[fetch] 重试 $i/10"; sleep 5; done; \
    [ "$ok" = "1" ] && cargo build --release --locked --bin cogneva

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

ARG APT_MIRROR_HOST

# 受限网络：ubuntu 源替换为镜像主机
RUN if [ -n "$APT_MIRROR_HOST" ]; then \
    sed -i "s|archive.ubuntu.com|$APT_MIRROR_HOST|g; s|security.ubuntu.com|$APT_MIRROR_HOST|g" \
    /etc/apt/sources.list /etc/apt/sources.list.d/*.sources 2>/dev/null || true; \
    fi

# Install runtime dependencies (ca-certificates for TLS, libssl3 for rustls)
# and Rust toolchain so the self-evolution worker can build patches inside K3s.
# libstdc++6：ONNX Runtime（fastembed/ort）动态链接依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libstdc++6 \
    curl \
    build-essential \
    git \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Install rustup/stable toolchain for the self-evolution worker.
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
ARG RUSTUP_DIST_SERVER
ARG RUSTUP_UPDATE_ROOT
ARG RUSTUP_INIT_URL
# CN 模式同上：TUNA 无 rustup-init.sh 脚本，直接拉 rustup-init 二进制
RUN if [ -n "$RUSTUP_DIST_SERVER" ]; then \
    export RUSTUP_DIST_SERVER RUSTUP_UPDATE_ROOT; \
    arch=$(uname -m); \
    curl --proto '=https' --tlsv1.2 -fsSL "$RUSTUP_UPDATE_ROOT/dist/$arch-unknown-linux-gnu/rustup-init" -o /tmp/rustup-init && \
    chmod +x /tmp/rustup-init && /tmp/rustup-init -y --default-toolchain stable; \
    else \
    curl --proto '=https' --tlsv1.2 -sSf "$RUSTUP_INIT_URL" | sh -s -- -y --default-toolchain stable; \
    fi && \
    /usr/local/cargo/bin/rustup component add rustfmt clippy

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
