# energy_dagster

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Next, make sure you have [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) installed. You can check this by running:

```bash
docker compose version
```

To initialize the database and to create the docker container, run:

```bash
docker compose up -d
```

You now need to create a `.env` file in your main repository and add credentials. The `.env` file will not be uploaded to git. 
To use the postgres IO manager, it should have the following content:

```
#
export pwd = your_password
export uid = your_username
export server = your_server
export db = your_database
export port = your_port
export schema = your_schema
export DBT_PROJECT_PATH = path/to/dbt_project
export DBT_PROFILES_PATH = path/to/dbt_profile
export MASTR_DOWNLOAD_DATE = today
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

If the environment variables were loaded successfully, you should see the following line:

```
dagster - INFO - Loaded environment variables from .env file: pwd,uid,server,db,port,schema, DBT_PROJECT_PATH, DBT_PROFILES_PATH, MASTR_DOWNLOAD_DATE
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `energy_dagster/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development


### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `energy_dagster_tests` directory and you can run tests using `pytest`:

```bash
pytest energy_dagster_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.