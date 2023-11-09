# Getting Started

`open-dapro` is the data orchestration and pipelining tool for the German Energy System built with [Dagster](https://dagster.io/){:target="_blank"}, [dbt](https://docs.getdbt.com/){:target="_blank"}, [PostGIS](https://postgis.net/){:target="_blank"}, and [GeoServer](https://geoserver.org/){:target="_blank"}. Its purpose is the collection and combination of data from different sources. The data pipelines can be scheduled regularly to keep the database up to date.

**Use `open-dapro` if you want to**

  * Have a local database of houses, electricity producers, ... on your server which updates automatically. 
  * Extend existing pipelines with the datasets you need.

## Installation
1. Clone the repository [git.fortiss.org/ASCI-public/energy-dagster](https://git.fortiss.org/ASCI-public/energy-dagster) and open it in a terminal.
1. Make sure you have [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/install/standalone/) installed by running 
```bash
docker --version
docker-compose --version
```
1. Run `docker-compose up` and go to [localhost:3000](localhost:3000){:target="_blank"} to see the dagster UI. A documentation of the dagster UI can be found [here](https://docs.dagster.io/concepts/webserver/ui){:target="_blank"}.
1. Start exploring the data pipelines :tada:

For an alternative setup you can also follow the instructions in the [Setup for development](development.md).

