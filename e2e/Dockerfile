FROM continuo-base:latest

# Install kubectl — detect arch at build time (arm64 for M-series Mac, amd64 for CI)
RUN ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/') && \
    KUBECTL_VERSION=$(curl -Ls https://dl.k8s.io/release/stable.txt) && \
    curl -Lo /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl" && \
    chmod +x /usr/local/bin/kubectl

WORKDIR /app
