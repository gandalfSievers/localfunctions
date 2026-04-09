# Stage 1: Generate TLS certificates with mkcert for *.amazonaws.com domains.
FROM alpine:3.20 AS cert-gen

RUN apk add --no-cache curl nss-tools

# Detect architecture at runtime for correct mkcert binary.
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
      MKCERT_ARCH="arm64"; \
    else \
      MKCERT_ARCH="amd64"; \
    fi && \
    curl -fsSL "https://github.com/FiloSottile/mkcert/releases/download/v1.4.4/mkcert-v1.4.4-linux-${MKCERT_ARCH}" \
      -o /usr/local/bin/mkcert && \
    chmod +x /usr/local/bin/mkcert

RUN mkcert -install && \
    mkdir -p /certs && \
    mkcert -cert-file /certs/cert.pem -key-file /certs/key.pem \
      "*.us-east-1.amazonaws.com" \
      "*.lambda.us-east-1.amazonaws.com" \
      "*.eu-west-1.amazonaws.com" \
      "*.lambda.eu-west-1.amazonaws.com" && \
    cp "$(mkcert -CAROOT)/rootCA.pem" /certs/rootCA.pem

# Stage 2: Traefik with certificates and configuration.
FROM traefik:v3.4

COPY --from=cert-gen /certs /certs
COPY traefik.yml /etc/traefik/traefik.yml
COPY dynamic.yml /etc/traefik/dynamic.yml
