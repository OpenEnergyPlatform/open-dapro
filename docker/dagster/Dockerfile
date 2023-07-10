# Dagster libraries to run both dagit and the dagster-daemon. Does not
# need to have access to any pipeline code.

FROM python:3.9-slim

RUN pip install \
    dagster \
    dagster-graphql \
    dagit \
    dagster-postgres \
    dagster-docker

# Set $DAGSTER_HOME and copy dagster instance and workspace YAML there
ENV DAGSTER_HOME=/opt/dagster/dagster_home/

RUN mkdir -p $DAGSTER_HOME

COPY docker/dagster/dagster.yaml $DAGSTER_HOME
COPY docker/dagster/workspace.yaml $DAGSTER_HOME

WORKDIR $DAGSTER_HOME