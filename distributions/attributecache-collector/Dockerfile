# Binary must be present in the build context as  otelcol-attributecache
# before running docker build.  The release workflow copies the appropriate
# platform binary here; for a manual build use the locally compiled binary.
FROM gcr.io/distroless/static-debian12:nonroot
COPY otelcol-attributecache /otelcol-attributecache
EXPOSE 4317 4318 4319 8888 55679
ENTRYPOINT ["/otelcol-attributecache"]
CMD ["--config", "/etc/otelcol/config.yaml"]
