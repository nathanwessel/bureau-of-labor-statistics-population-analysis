# Process

This document describes the main architectural decisions, trade-offs, and challenges I worked through while building the project.

## Architecture

### Bronze, Silver, and Gold

I separated the pipeline into Bronze, Silver, and Gold layers based on the responsibility of each layer.

****Bronze**** - stays as close as practical to the source and preserves the data needed to reproduce what was ingested. I intentionally avoid applying business logic here.

****Silver**** - turns the source data into typed, validated, reusable datasets. This is where I normalize column names and data types, enforce important data-quality expectations, and model the BLS data as facts and dimensions without prematurely joining everything together.

****Gold**** - applies the analytical logic needed to answer the business questions. I kept this logic out of Silver so the Silver tables remain reusable for questions other than the three in the assignment.

For example, the BLS Silver fact tables retain the identifiers supplied by BLS, while Gold joins the appropriate descriptive dimensions when a human-readable series label is needed.

For Question 3, I also created a reusable Gold table containing population alongside every available BLS series, period, and year before creating the assignment-specific result for `PRS30006032` and `Q01`. This avoids embedding the assignment filter too early in the data model.

### PySpark and SQL

PySpark is the primary implementation for most transformations. Python is my strongest production language, and it also made factory-style transformations much cleaner.

That was particularly useful for the BLS datasets. Rather than writing nearly identical transformation code for every BLS source table, I could represent the differences as configuration and use reusable functions to create the tables consistently.

I also included SQL implementations where requested by the assignment. The SQL versions provide an alternative representation of the same analytical logic, while PySpark is the primary implementation.

### Dynamic BLS source discovery

I did not want the ingestion process to depend on manually maintaining a list of only the files needed for the three assignment questions.

The BLS ingestion discovers the published source files in the productivity time-series directory and handles the known non-data files separately. This makes the ingestion layer more reusable and means that the pipeline is not coupled only to the small subset of BLS data currently needed by Gold.

I kept the corresponding Bronze and Silver structures relatively similar to the BLS source instead of combining the datasets into a highly-customized model at ingestion time.

### Population snapshots and safe reruns

The population API is treated as a snapshot source.

Each successful API fetch is stored as its own timestamped, immutable JSON file. I chose this design primarily for reproducibility and production debugging. If a problem is discovered later, the exact source response that was available when the pipeline ran still exists. That is much more useful than having to guess what an external API may have returned at that time.

Persisting the raw API response before transforming it also separates source retrieval from the data pipeline itself. If a downstream issue needs to be reproduced, I can rerun the transformations against the exact response that was originally fetched without depending on the external API to return the same data again.

The Bronze population table uses Databricks Auto Loader to incrementally discover those snapshot files:

```python
spark.readStream 
    .format("cloudFiles") 
    .option("cloudFiles.format", "binaryFile")
```

This allows already-ingested files to remain unchanged while newly arriving snapshots are processed incrementally.

Silver interprets the population API as a current-state snapshot and selects the newest available response before exploding the JSON into normal tabular rows. As a result, rerunning the downstream transformations does not create duplicate analytical population records from all of the historical fetches.

This separates two concerns:

- Bronze retains what was actually fetched.

- Silver represents the latest usable state of the population dataset.

The BLS ingestion follows a similar separation between source acquisition and downstream transformation, while dynamically discovering the current files published by BLS rather than hardcoding the analytical questions into ingestion.

### Data-quality expectations

When deciding whether a data-quality condition should fail a pipeline or merely be exposed to the consumer, I generally used the following principle:

> Fail when the violation makes the resulting dataset unsafe or ambiguous to consume. Flag conditions that may be legitimate but are still useful to surface.

Examples of conditions that justify failure include malformed required structures or invalid required keys.

Other conditions are better represented explicitly in the data rather than automatically rejected. The BLS quarterly data was an important example of this distinction because not every series/year has four quarterly observations.

## BLS yearly-value logic

Question 2 required the most care because BLS publishes both quarterly observations and `Q05` observations.

`Q05` represents an annual average. When quarterly observations exist, I prefer the underlying quarterly values and calculate the yearly value by summing the available quarters. (I do not also include `Q05`, because it describes the same year using a different annual measure and should not be added to the quarterly observations.)

For a year with no quarterly observations but a `Q05` value, I use `Q05` as the usable yearly value.

The resulting rule is:

1. If quarterly observations exist, sum the available `Q01` through `Q04` values.

2. If no quarterly observations exist and `Q05` exists, use the `Q05` value.

3. Preserve information about whether all four quarters were present.

I intentionally do not exclude a year merely because fewer than four quarters were published. If the sum of the available quarters makes that year the largest value for a series, then it should still be represented as a winning year. Removing it would introduce an assumption that the assignment did not specify and would discard a real result from the supplied data.

I also preserve ties. If multiple years have the same maximum yearly value for a series, all of those years are returned. A ranking implementation that arbitrarily chooses one tied year loses valid information that a business stakeholder would be interested in knowing.

## Trade-offs for a real client

The assignment datasets are small enough that I favored clarity and correctness over implementing optimizations that are not currently necessary. There are several areas I would expand for a production workload.

### Schema evolution

I would keep Bronze deliberately tolerant of source changes. Most Bronze values can remain close to their source representation, often as strings, so an upstream additive change does not unnecessarily stop ingestion.

Silver and Gold would have stricter contracts. A breaking schema change that invalidates downstream assumptions should fail clearly rather than silently producing incorrect analytical data.

For a production implementation, I would also add explicit alerting around source schema changes so that additive changes can be reviewed before intentionally incorporating them downstream.

### Performance and scale

At the current data volume, aggressive Spark optimization would add complexity without providing much value.

At larger scale, I would profile the transformations and optimize where the execution plan showed that it was warranted. Depending on the workload, that could include:

- appropriate caching where the same expensive intermediate result is reused;

- partitioning or repartitioning based on access and join patterns;

- examining Spark explain plans;

- identifying unnecessary shuffles;

- reviewing join strategies and data distribution.

### Monitoring

For a real client, I would add operational alerting around the pipelines.

At minimum, I would want Slack or Microsoft Teams notifications for pipeline failures, including failures caused by data-quality expectations. I would also add freshness or SLA alerts so that a pipeline that technically succeeds but does not produce data within the expected window is still treated as an operational issue.

### Environments

For a production implementation, I would also separate development, test, and production resources rather than treating one catalog and schema hierarchy as the complete deployment environment.

The project already begun efforts centralizing catalog and schema names in configuration rather than repeating identifiers such as `rearc.bronze`, `rearc.silver`, and `rearc.gold` in transformation code. That effort begun there makes environment-specific configuration significantly easier.

## Retrospective

One of the hardest parts of the assignment was proving that the aggregation represented the source data correctly.

While inspecting the BLS data, I found that not every series/year has all four quarters recorded, particularly in parts of the historical data. That meant a simple assumption that every valid year must contain `Q01`, `Q02`, `Q03`, and `Q04` would incorrectly reject real BLS observations.

I therefore changed the yearly-value logic to explicitly account for available quarters and annual-only `Q05` observations rather than treating incomplete quarterly coverage as an automatic failure.

I spent additional time testing that logic with deliberately constructed cases.

I utilized test cases covering:

- a normal series with four quarterly observations plus `Q05`, verifying that the quarterly sum is used;

- an incomplete year, verifying that the available quarters remain usable;

- a `Q05`-only series, verifying that the annual observation is used when quarters do not exist;

- tied best years, verifying that every valid winning year is retained.

Tracing through the Bronze, Silver, and expected Gold schema with these was was useful because it tested the business rules themselves rather than only confirming that the pipeline executed successfully.

Another important part of the work was keeping the implementation maintainable as it evolved. For example, I moved catalog and schema names out of the PySpark transformation files and into shared configuration, clarified Gold table names, removed unnecessary columns from final analytical outputs, and adjusted DataFrame and column organization to make the transformations easier to read.

The areas I am most satisfied with are the dynamic BLS source discovery, the population raw-history design, and the handling of the BLS quarterly versus annual semantics. Those were the areas where a solution that merely "ran successfully" could still have produced a misleading result.

## AI usage

I used ChatGPT as a reference, drafting, and refactoring tool throughout the project. I reviewed the generated code and made changes when the proposed implementation did not match the source data, the assignment requirements, or the way I wanted the project structured.

Some examples of changes I made after drafting code with AI include:

- removing extraneous code and unnecessary columns, including source-file metadata that had been carried into Gold outputs;

- correcting and clarifying table names;

- retaining `Nation ID` and `Nation` where they were useful in the population Gold data;

- manually debugging BLS records that were initially being flagged because some years do not contain all four quarters, then changing the yearly-value logic to account for the actual data;

- replacing a configuration dictionary that did not need values with a simpler list;

- replacing an unnecessarily indirect join used to select the latest population snapshot with a clearer filter-based implementation;

- standardizing DataFrame naming, column ordering, comments, and general readability;

- refining Gold table names and outputs so that they more clearly represent the business result