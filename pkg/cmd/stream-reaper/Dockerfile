# The stream-reaper lives in the self-contained pkg module, so it builds in
# module mode from the pkg build context (no go.work required). Build context
# is `pkg/`.
ARG BASE_IMAGE=ghcr.io/carolsimone/continuo-base:v1
FROM --platform=$BUILDPLATFORM ${BASE_IMAGE} AS builder

# Supplied by buildx: the architecture of the image being produced. The builder
# itself stays on the native build platform and cross-compiles to it.
ARG TARGETARCH

WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -a -o /app/bin/stream-reaper ./cmd/stream-reaper

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app/bin/stream-reaper /usr/local/bin/stream-reaper

# Static binary; no filesystem writes at runtime — run unprivileged.
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/stream-reaper"]
