# syntax=docker/dockerfile:1
FROM rust:1.89-bookworm@sha256:948f9b08a66e7fe01b03a98ef1c7568292e07ec2e4fe90d88c07bb14563c84ff AS build
ARG CARGO_BUILD_JOBS=2
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY web ./web
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo test --locked && cargo build --locked --release

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
ARG VERSION=0.1.14
ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="Genius Trader" \
      org.opencontainers.image.description="Binance USD-M futures service with a local Chinese console and Jev decisions" \
      org.opencontainers.image.source="https://github.com/zuoban/GeniusTrader" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 trader \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin trader \
    && install -d -m 0700 -o trader -g trader /app/data
COPY --from=build /build/target/release/genius-trader /usr/local/bin/genius-trader
WORKDIR /app
USER 10001:10001
VOLUME ["/app/data"]
EXPOSE 8787
STOPSIGNAL SIGINT
ENTRYPOINT ["/usr/local/bin/genius-trader"]
CMD ["--db", "/app/data/trader.sqlite", "--bind", "0.0.0.0"]
