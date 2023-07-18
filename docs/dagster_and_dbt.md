# Dagster and dbt
This project builds upon two open-source frameworks: [Dagster](https://dagster.io/) and [dbt](https://docs.getdbt.com/). We use the data orchestration tool `Dagster` to build and schedule our pipelines. Inside `Dagster`, many of the actual data transformations are implemented using `dbt`.
Both frameworks are documented extensively. Before you dive into `energy-dagster`, make sure that you have a basic understanding of the two frameworks.

!!! tip

    If you understand how to use Dagster and dbt, you will also understand `energy-dagster`.

```mermaid
flowchart TB
    subgraph Dagster
    id1.1(raw tabular data)-->id2[(Postgis database)]
    id1.2(images)-->id1.3[ML model]
    id1.3-->id1.4(tabular data)
    id1.4-->id2
    id1.5(...)-->id2
    subgraph dbt
    id2-->|transformation|id2
    end
    id2-->id3[Vizualization and Analysis]
    end
```

We use `Dagster` to:

  * **Implement** the code to download raw data
  * **Save data** to databases or files 
  * **Schedule** pipeline executions

We use `dbt` to:

  * **Transform and merge** data to obtain final output tables


