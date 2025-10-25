FROM alpine:3.19

RUN apk add --no-cache \
        pgbouncer \
        bash \
        postgresql15-client \
        su-exec

RUN addgroup -S pgbouncer \
    && adduser -S -D -G pgbouncer pgbouncer \
    && mkdir -p /etc/pgbouncer /var/log/pgbouncer \
    && chown -R pgbouncer:pgbouncer /etc/pgbouncer /var/log/pgbouncer

COPY docker-entrypoint.sh /usr/local/bin/pgbouncer-entrypoint.sh

RUN chmod +x /usr/local/bin/pgbouncer-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/pgbouncer-entrypoint.sh"]
