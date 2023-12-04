FROM python:3.9-slim
# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to webserver and dagster-daemon, and to load the DagsterInstance

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker

COPY .  /opt/dagster/app

WORKDIR /opt/dagster/app

RUN pip install -e . 


# Run dagster gRPC server on port 4000

EXPOSE 4000

