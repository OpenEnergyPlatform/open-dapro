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
RUN /bin/sh -c "cd dbt && dbt deps"

# Run dagster gRPC server on port 4000

EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "energy_dagster"]
#CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "--working-directory", "."]