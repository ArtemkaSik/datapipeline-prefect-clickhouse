FROM apache/superset:latest

USER root
RUN python -m ensurepip --upgrade \
    && python -m pip install --no-cache-dir "clickhouse-connect>=0.13.0"
USER superset
