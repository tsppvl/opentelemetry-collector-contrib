# Two arch-specific binaries must be present in the build context before
# running docker buildx build --platform linux/amd64,linux/arm64:
#   otelcol-attributecache_amd64
#   otelcol-attributecache_arm64
# The release workflow copies them; for a manual build compile each arch
# with CGO_ENABLED=0 GOOS=linux GOARCH=<arch> and place them here.
FROM --platform=$BUILDPLATFORM alpine:latest@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11 AS prep
RUN apk --update add ca-certificates

FROM scratch

ARG USER_UID=10001
ARG USER_GID=10001
ARG TARGETARCH
USER ${USER_UID}:${USER_GID}

COPY --from=prep /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY otelcol-attributecache_${TARGETARCH} /otelcontribcol
EXPOSE 4317 55680 55679
ENTRYPOINT ["/otelcontribcol"]
CMD ["--config", "/etc/otel/config.yaml"]
