FROM apache/superset:latest

# Install ClickHouse drivers + dialect + Superset DB engine plugin
RUN pip install --no-cache-dir \
      clickhouse-driver \
      clickhouse-sqlalchemy \
      superset-clickhouse

# Explicitly import the ClickHouse Engine Spec so Superset registers it
ENV SUPERSET_ADDITIONAL_MODULES="\
superset_clickhouse.plugins.clickhouse.queries,\
superset_clickhouse.plugins.clickhouse.engine,\
superset_clickhouse.plugins.clickhouse.forms"

# Tell Superset to load the ClickHouse engine plugin
ENV SUPERSET_PLUGINS="superset_clickhouse.plugin"
