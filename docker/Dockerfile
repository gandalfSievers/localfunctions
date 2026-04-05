# syntax=docker/dockerfile:1

# ---------- builder ----------
FROM rust:1.88-bookworm AS builder

# Install cross-compilation toolchains for multi-arch builds.
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc-aarch64-linux-gnu \
        gcc-x86-64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*

# Map Docker TARGETARCH to Rust target triple.
RUN case "${TARGETARCH}" in \
        arm64) echo "aarch64-unknown-linux-gnu" > /tmp/rust_target ;; \
        *)     echo "x86_64-unknown-linux-gnu"  > /tmp/rust_target ;; \
    esac && \
    rustup target add "$(cat /tmp/rust_target)"

# Configure the linker for each target.
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc

WORKDIR /build

# Cache dependency builds: copy manifests first, build a dummy lib, then copy
# real sources and rebuild only the application code.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs && \
    cargo build --release --target "$(cat /tmp/rust_target)" && \
    rm -rf src

COPY src/ src/
RUN touch src/main.rs src/lib.rs && \
    cargo build --release --target "$(cat /tmp/rust_target)" && \
    cp "target/$(cat /tmp/rust_target)/release/localfunctions" /build/localfunctions

# ---------- runtime ----------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Run as non-root user.
RUN groupadd --system app && useradd --system --gid app --create-home app

COPY --from=builder /build/localfunctions /usr/local/bin/localfunctions
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 9600 9601

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:9600/health || exit 1

USER app

ENTRYPOINT ["entrypoint.sh"]
