FROM alpine:3.20

# turbodb binary is statically linked (musl). Alpine gives us /tmp, /bin/sh
# for diagnostic exec-into and apk for ad-hoc tooling without bloating the image.
COPY zig-out/bin/turbodb /usr/local/bin/turbodb

RUN mkdir -p /var/lib/turbodb
WORKDIR /var/lib/turbodb

EXPOSE 27017 27018

ENTRYPOINT ["/usr/local/bin/turbodb"]
CMD ["--data", "/var/lib/turbodb", "--port", "27017", "--both"]
