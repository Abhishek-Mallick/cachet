# One Dockerfile, one image per binary, selected by BINARY.
#
# Separate images rather than one image with four entrypoints: a sidecar should carry the engine and
# nothing else. A shared image would put the operator CLI and the verifier into every pod that runs
# the read path, which is more attack surface than a sidecar has any reason to hold.

# ---- build ----------------------------------------------------------------------------------
FROM golang:1.27.1-alpine AS build

# Dependencies first, so a source-only change does not re-download the module cache.
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG BINARY=cachet
ARG VERSION=dev
# CGO off and a static link, because the runtime stage has no libc at all. -trimpath so the binary
# does not embed the build machine's paths, which is both noise and a small information leak.
RUN CGO_ENABLED=0 go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/app ./cmd/${BINARY}

# ---- runtime --------------------------------------------------------------------------------
# Distroless static: no shell, no package manager, nothing to exec if something does get in.
FROM gcr.io/distroless/static-debian12:nonroot

ARG BINARY=cachet
ARG VERSION=dev

LABEL org.opencontainers.image.title="cachet-${BINARY}" \
      org.opencontainers.image.description="Cachet — an integrated read cache for sharded OLTP databases" \
      org.opencontainers.image.source="https://github.com/Abhishek-Mallick/cachet" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=build /out/app /usr/local/bin/cachet-app

# Already nonroot via the base image tag; stated explicitly so a base image change cannot silently
# promote this to root.
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/cachet-app"]
