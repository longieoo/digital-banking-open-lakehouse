# Build the discontinued community image tags from their corresponding upstream commits.
FROM golang:1.24.2-bookworm AS build
ARG PROJECT
ARG REVISION
WORKDIR /src
RUN curl -fsSL --retry 3 "https://codeload.github.com/minio/${PROJECT}/tar.gz/${REVISION}" \
    | tar -xz --strip-components=1
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -p 2 -trimpath -o /out/binary .

FROM debian:bookworm-slim
ARG PROJECT
ARG REVISION
LABEL org.opencontainers.image.source="https://github.com/minio/${PROJECT}" org.opencontainers.image.revision="${REVISION}"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/binary /usr/bin/${PROJECT}
COPY --from=build /src/LICENSE /usr/share/doc/storage/LICENSE
RUN ln -s "/usr/bin/${PROJECT}" /usr/local/bin/storage
ENTRYPOINT ["/usr/local/bin/storage"]
