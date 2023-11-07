[![Documentation](https://github.com/OpenEnergyPlatform/open-dapro/actions/workflows/gh-pages.yml/badge.svg)](https://openenergyplatform.github.io/open-dapro/)

# open-dapro: Data Processing and Pipelining for the German Energy System 

open-dapro is a collection of automated data pipelines for the German energy system built with [Dagster](https://dagster.io/) and [dbt](https://www.getdbt.com/).


## Installation

### Installation as a developer

1. First, install your Dagster code location as a Python package. By using the -e (editable) flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

2. Next, make sure you have [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) installed. You can check this by running:

```bash
docker compose version
```


3. You now need to rename the `.env.template` file to `.env` and change your credentials if needed. The `.env` file will not be uploaded to git. Note that these credentials have to match the database created with the `docker-compose.yml` file. The default credentials are:


```
#
export pwd = postgres
export uid = postgres
export server = localhost
export db = energy_database
export port = 5511
export schema = raw
export DAGSTER_HOME = ~/.dagster/dagster_home
export DBT_PROFILE_FOLDER = dev
export MASTR_DOWNLOAD_DATE = today
```

4. To initialize the database and to create the docker container, run:

```bash
python development/initialize.py
```
Check if the database is running on the server and port specified in the `.env` file. 

5. Start the Dagster UI web server:

```bash
dagster dev
```

If the environment variables were loaded successfully, you should see the following line:

```
dagster - INFO - Loaded environment variables from .env file: pwd,uid,server,db,port,schema, MASTR_DOWNLOAD_DATE
```

6. Open http://localhost:3000 with your browser to see the project.

You can start writing your own assets in `energy_dagster/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

### Installation with docker compose

Make sure you have [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) installed. Pull the repository, enter it, and run
```bash
docker-compose up
```

## Development
### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.


### pre-commit hooks
In this project, we use [pre-commit hooks](https://pre-commit.com/) to lint the code before committing. The hooks are defined in the `.pre-commit-config.yaml` file. To install the hooks, run the following command:

```bash
pre-commit install
```

This will install the hooks in your local repository. They will be executed before every commit and check for linting errors using the [sqlfluff](https://docs.sqlfluff.com/en/stable/) and [black](https://black.readthedocs.io/en/stable/) packages.

### dbt osmosis
You can use `dbt-osmosis` for creating, updating, and deleting dbt property files.
This can be done using the following command:

```
dbt-osmosis yaml refactor .\models\marts\
```

## Acknowledgement 
This repository was developed at [fortiss GmbH]()
