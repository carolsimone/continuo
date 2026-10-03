# Benchmark stand-in for a team dbt image. `dbt` is a shell script that prints a
# line and exits at once (or after SLEEP_SECONDS); tables named fail_* fail.
# Used only by scripts/bench.
FROM busybox:1.37.0
ARG SLEEP_SECONDS=0
ENV BENCH_SLEEP_SECONDS=${SLEEP_SECONDS}
COPY --chmod=0755 dbt /usr/local/bin/dbt
USER 65532:65532
