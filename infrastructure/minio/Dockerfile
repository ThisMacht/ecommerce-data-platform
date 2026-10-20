FROM golang:1.24.8-alpine3.22 AS build
WORKDIR /build
# MinIO RELEASE.2025-10-15T17-29-55Z. Build from verified upstream source;
# this release does not provide an official prebuilt container image.
ADD --checksum=sha256:45521908307306e925c98d629e1c17d78c8b72b6ee242b1bfb1409f7d8ee5841 https://codeload.github.com/minio/minio/tar.gz/9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a /tmp/minio.tar.gz
RUN tar -xzf /tmp/minio.tar.gz --strip-components=1 && rm /tmp/minio.tar.gz
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOMAXPROCS=2
RUN go build -p 2 -mod=readonly -trimpath -ldflags="-s -w" -o /out/minio .

FROM alpine:3.22.2
RUN apk add --no-cache ca-certificates curl
COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /build/LICENSE /usr/share/licenses/minio/LICENSE
LABEL org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.revision="9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a" \
      org.opencontainers.image.licenses="AGPL-3.0-only"
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
