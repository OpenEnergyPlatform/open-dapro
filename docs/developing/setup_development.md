# Setup for development

1. Clone the repository [git.fortiss.org/ASCI-public/energy-dagster](https://git.fortiss.org/ASCI-public/energy-dagster) and open it.
1. Install your Dagster code location as a Python package. By using the -e (editable) flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

    ```bash
    pip install -e ".[dev]"
    ```

1. Next, make sure you have [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/install/standalone/) installed. You can check this by running:

    ```bash
    docker-compose --version
    ```


1. You now need to rename the `.env.template` file to `.env` and change your credentials if needed. The `.env` file will not be uploaded to git. Note that these credentials have to match the database created with the `docker-compose.yml` file. 

1. To initialize the database and to create the docker container, run:

    ```bash
    python development/initialize.py
    ```
    Check if the database is running on the server and port specified in the `.env` file. 

1. Start the Dagster UI web server:

    ```bash
    dagster dev
    ```

    If the environment variables were loaded successfully, you should see the following line:

    ```
    dagster - INFO - Loaded environment variables from .env file: pwd,uid,server,db,port,schema, MASTR_DOWNLOAD_DATE
    ```

1. Open [http://localhost:3000](http://localhost:3000) with your browser to see the project. You can start writing your own assets in `energy_dagster/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.