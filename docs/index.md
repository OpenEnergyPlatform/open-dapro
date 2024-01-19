# Getting Started

`open-dapro` is the data orchestration and pipelining tool for the German Energy System built with [Dagster](https://dagster.io/){:target="_blank"}, [dbt](https://docs.getdbt.com/){:target="_blank"}, [PostGIS](https://postgis.net/){:target="_blank"}, and [GeoServer](https://geoserver.org/){:target="_blank"}. Its purpose is the collection and combination of data from different sources. The data pipelines can be scheduled regularly to keep the database up to date.

**Use `open-dapro` if you want to**

  * Have a local database of houses, electricity producers, ... on your server which updates automatically. 
  * Extend existing pipelines with the datasets you need.

## Installation
1. Clone the repository [github.com/OpenEnergyPlatform/open-dapro](https://github.com/OpenEnergyPlatform/open-dapro) and open it in a terminal.
1. Make sure you have [docker](https://www.docker.com/) installed by running 
1. Rename the `.env.template` file to `.env` and change the defined variables as you wish. Afterwards, run 
  ```bash
  docker compose --env-file .env up
  ```
  If everything worked, the GeoServer should be available at [localhost/geoserver](http:localhost/geoserver) with credentials `admin:geoserver` and the Dagster UI at [localhost/dagster](http:localhost/dagster) with credentials `dagster-admin:admin`.
1. Start exploring the data pipelines :tada:

For an alternative setup you can also follow the instructions in the [Setup for development](development.md).

