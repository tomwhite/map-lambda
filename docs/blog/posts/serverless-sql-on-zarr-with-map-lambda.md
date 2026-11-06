---
draft: false
date: 2026-11-06
authors:
  - tomwhite
---

# Serverless SQL on Zarr with map-lambda

*Map-lambda isn't just good for rechunking - it can do SQL too! Two geospatial SQL queries over 3TB of real ERA5 data in S3 each ran from cold in a couple of minutes and cost a couple of dollars on AWS.*

<!-- more -->

I have long been interested in using serverless technologies in large-scale data processing applications. In an ideal world, it would be easy to query large datasets *without* having to manage a cluster. Starting (and stopping) a cluster to answer a couple of queries introduces real friction into the whole process.

So, I wondered, is it possible to use map-lambda to run a meaningful SQL query on a large Zarr dataset?

For the last couple of years, Alex Merose and others have been exploring the idea of using SQL to query Zarr datasets on the [xql.systems](https://xql.systems/) project. Their pioneering work - in the form of the [zarr-datafusion](https://github.com/stratoscale-io/zarr-datafusion) project (developed by Jayendra Parmar), plus the [map-lambda](https://github.com/tomwhite/map-lambda) project I've been working on here - turned out to be the two ingredients needed to make serverless SQL for Zarr a reality.

### The results

Before looking at how it works, here are the headline results from the [benchmarks](https://github.com/tomwhite/datafusion-lambda-benchmarks) I ran:

**The monthly area-weighted global mean temperature over 85 years of hourly
ERA5 (745,128 chunks, 3.1 TB uncompressed) takes ~88s and costs ~$2.17**

**The hourly climatology of every grid cell over the same ERA5 record, written to
Zarr on S3, takes ~136s and costs ~$3.07**

These are both real queries over non-trivial amounts of data, using a standard AWS account with the default lambda concurrency quota (1000 instances). Both executed from cold - in other words, you can have the Lambda functions deployed on your AWS account and only pay for them when you use them to execute a query against your Zarr data in S3.

### The idea behind zarr-datafusion

The [xql.systems](https://xql.systems/) community was founded on the idea that you can "pivot" array datasets to treat them like tables, and then run SQL queries on the tables. The results can then be pivoted back to their array form. All the difficult work of parsing SQL, query planning and execution is delegated to the underlying database engine, like [Apache DataFusion](https://datafusion.apache.org/) or [DuckDB](https://duckdb.org/).

The [xarray-sql](https://xql.systems/xarray-sql/latest/) project implements this idea for Xarray datasets, while [zarr-datafusion](https://github.com/stratoscale-io/zarr-datafusion) does the same for Zarr arrays. There is a lot of interest in the community at the moment around querying Zarr with SQL: [Earthmover's Zax](https://www.earthmover.io/blog/compute-roadmap) is a new query engine that has a SQL frontend.

### Parallel execution with map-lambda

Going back to the benchmarks, here is the query for calculating the global mean:

```sql
SELECT date_trunc('month', CAST(valid_time AS TIMESTAMP))              AS month,
       sum(t2m * cos(radians(latitude))) / sum(cos(radians(latitude))) AS mean_t2m,
       min(t2m)                                                        AS min_t2m,
       max(t2m)                                                        AS max_t2m,
       count(*)                                                        AS n
FROM era5
WHERE valid_time >= TIMESTAMP '{start}' AND valid_time < TIMESTAMP '{end}'
GROUP BY 1
ORDER BY 1
```

The `era5` table in the query is registered as an external table pointing at the Icechunk store at `s3://earthmover-icechunk-era5/icechunkV2` (a dataset generously hosted by Earthmover in the `us-east-1` region).

The query will touch all of the Zarr chunks in the time range, but the aggregated data in the result is small. It's possible to run it on a single machine, and zarr-datafusion will do that, but it would be slow over the full 85 years and you really want to throw more machines at it in that case. The full dataset has 745,128 chunks, so you really don't want to download them all to one machine!

Since zarr-datafusion is written in Rust, it's an ideal candidate for working with map-lambda, which fans out work to Rust lambdas. The hard bit of the integration was to add a dedicated operator to DataFusion's physical plan that does the fan-out. `MapLambdaZarrExec` is used to run partial
aggregates on Lambda workers, reading directly from Zarr chunks and pivoting into tabular form. For the global means, the partial aggregate results for each worker are streamed back to the client which computes the final values (the monthly mean and other stats).

The climatology query produces more data, since it is computing the average value for each hour of day at each location in the dataset. Streaming the results back to the client doesn't make sense in this case, so instead, each worker performs the aggregation, then writes a chunk to an output Zarr array. Here's the query to do that:

```sql
COPY (
  SELECT latitude, longitude,
         date_part('hour', CAST(valid_time AS TIMESTAMP)) AS hour,
         avg(t2m) AS clim, count(*) AS n
  FROM era5
  WHERE valid_time >= TIMESTAMP '{start}' AND valid_time < TIMESTAMP '{end}'
  GROUP BY 1, 2, 3
) TO 's3://<bucket>/climatology.zarr' STORED AS ZARR OPTIONS ('chunks' '24,12,12')
```

More details and all the code used to run the benchmarks are available in the [datafusion-lambda-benchmarks repo](https://github.com/tomwhite/datafusion-lambda-benchmarks).

### What's next?

It's important to stress that this work is a proof of concept. The code in the benchmarks repo does not constitute a general purpose SQL query engine. Both queries are aggregation queries (using `GROUP BY`), but there are several other types of query you could perform on the same dataset, which would *not* be turned into a plan that fans out across workers.

A useful list of the categories of query that a more fully-featured SQL engine would provide is covered in xarray-sql's guide to [Geospatial in SQL](https://xql.systems/xarray-sql/latest/geospatial/#the-mapping). The serverless benchmark covers only *one* of the nine operations (climatology), and considerable work is needed for each of the other operations.

So while it's heartening that the goal of serverless SQL on Zarr is certainly feasible, more work is needed.
