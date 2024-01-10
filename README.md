[![Documentation](https://github.com/OpenEnergyPlatform/open-dapro/actions/workflows/gh-pages.yml/badge.svg)](https://openenergyplatform.github.io/open-dapro/)

# open-dapro: Data Processing and Pipelining for the German Energy System 

open-dapro is a collection of automated data pipelines for the German energy system built with [Dagster](https://dagster.io/) and [dbt](https://www.getdbt.com/).


## Installation

Make sure you have [docker](https://www.docker.com/) installed. Clone the repository, enter it, and run
```bash
docker compose up
```

In case you want to change default variables, rename the `.env.template` file to `.env` and change the defined variables as you wish. Afterwards, run 
```bash
docker compose --env-file .env up
```

If everything worked, the GeoServer should be available at [http://127.0.0.1:8002](http://127.0.0.1:8002) and the Dagster UI at [http://127.0.0.1:3000](http://127.0.0.1:3000)

To install the project in a developer setup check the [Setup for Developers](https://openenergyplatform.github.io/open-dapro/development/) on the documentation page.


## Acknowledgement 
This repository was developed at [fortiss GmbH](https://www.fortiss.org/) within the [eTwin.BY Project](https://www.fortiss.org/en/research/projects/detail/etwinby). This work was funded by "Bayerische Staatsministerium für Wirtschaft, Landesentwicklung und Energie" as part of "Bayerischen Verbundförderprogramms (BayVFP) – Förderlinie Digitalisierung – Förderbereich Informations- und Kommunikationstechnik".
<div align="center">
  <img src="https://github.com/OpenEnergyPlatform/open-dapro/assets/74312290/e88b3c4e-4c58-41cf-9b61-a8ff591bdeea" alt="Logo of the Research Project eTwin.BY" width="200" height="200">
</div>
