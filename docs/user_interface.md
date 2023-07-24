# The Dagser User Interface

The data pipelines and the materialized data assets (tables and files) together with metadata and descriptions are shown in the Dagster UI. The Dagster UI itself is documented on the [official website](https://docs.dagster.io/concepts/webserver/ui).

## data documentation
We chose to not document the datasets in this documentation page for the two reasons:
1. We do not want to have the same content twice. Most of the data is already documented within the code.
1. We want this documentation to be as simple as possible - both for readers and maintainers.

However, the created datasets are documented within the dagster UI. When clicking on a chosen asset, one can often find descriptive statistics or metadata like the `table_schema`.

