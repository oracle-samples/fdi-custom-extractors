# Sample Code to Build a Pipeline with Source Data in FDI's TCF (Table Change Format)

- [1. Overview](#1-overview)
- [2. Brief Overview of Table Change Format (TCF)](#2-brief-overview-of-table-change-format-tcf)
- [3. TCF Pipeline Code to Build a Data Pipeline](#3-tcf-pipeline-code-to-build-a-data-pipeline)
- [4. Setup of TCF Data Pipeline in OCI DataFlow (OCI's Managed Spark Environment)](#4-setup-of-tcf-data-pipeline-in-oci-dataflow-ocis-managed-spark-environment)
- [5. TCF Reader Library - Interface Overview](#5-tcf-reader-library---interface-overview)
- [6. FDI's TCF Reader Library](#6-fdis-tcf-reader-library)

## 1. Overview

This solution has two parts/modules:

1. A module that helps read and consume FDI's change data feed in TCF (Table Change Format).
2. TCF pipeline sample code to build a data pipeline that can consume TCF change sets and apply them to popular destinations such as data lakes and warehouses.

## 2. Brief Overview of Table Change Format (TCF)

1. Hierarchical organization and versioning (Source → dataset → SCN → data)
2. Schema and partitioning (optional)
3. Comprehensive change tracking (Insert, Update, Delete)
4. Data readiness and integration support (a completion `DONE` file signals ready-to-consume data)
5. Markers to easily reference the most recent change or complete dataset for processing and integration

## 3. TCF Pipeline Code to Build a Data Pipeline

TCF pipeline code helps build a data pipeline that can consume data in FDI's Table Change Format (TCF) and write data to Delta Lake.

Let's look at the core functionality of this pipeline:

1. Initializes the pipeline, including source, sink, and watermark store.
2. Identifies the list of datasets in the source.
3. For each dataset:
    1. Reads pending ChangeIds (SCNs) to be processed.
    2. For each pending ChangeId (SCN):
        - Reads changesets (Inserts, Updates, Deletes) associated with the specific SCN.
        - Applies these changesets to the sink/destination.
        - Commits this SCN to the watermark for this dataset.

This core pipeline logic exists in the `TcfPipeline` class.

The pipeline consists of 3 major components:

### 3.1 Source

- Source will be initialized based on the config in `pipeline_manifest.json -> source`.
- The source format here is FDI's Table Change Format (TCF).
- Source storage can be:
    - OCI Object Storage
    - Local filesystem

One key design goal is to make reading TCF data look and behave like any standard Spark datasource.

Typical read example:

```java
Dataset<Row> changesets = sparkSession.read()
            .format("fdi-tcf")
            .option("startingScn", startingScn)
            .option("endingScn", endingScn)
            .load(datasetRootPath);
```

Under the hood, `"fdi-tcf"` is implemented as a Spark `DataSourceV2` datasource that understands:

- SCN metadata
- Marker files
- Schema evolution
- Changeset organization
- Partition pruning

From Spark's perspective, however, it behaves like any other datasource and integrates with:

- Catalyst optimizer
- Predicate pushdown
- AQE (Adaptive Query Execution)
- Spark logical and physical planning

The datasource uses marker files maintained within the TCF dataset to resolve
the available SCN range:

- `latestFullRefreshSCN`
- up to `latestSCN`

Optional `startingScn` and `endingScn` values narrow the read within that
marker-based range.

### 3.2 Sink

- Sink will be initialized based on the config in `pipeline_manifest.json -> sink`.
- Sink data format currently supported in this sample is Delta Lake.
- Sink storage can be:
    - OCI Object Storage
    - Local filesystem

The sink is responsible for applying inserts, updates, and deletes from each SCN to the destination system.

Typical flow:

```java
WriteSummary result =
    sink.applyChangeSets(
        datasetName,
        changeset,
        schema
    );
```

The pipeline itself remains independent of the sink implementation and only expects a success/failure contract.

### 3.3 Watermark

Watermark tracking maintains how far SCNs/change sets have been successfully applied to the sink.

Purpose:

- Tracks the last applied SCN
- Identifies pending SCNs
- Supports resumability and restartability
- Ensures incremental processing

Default implementation:

- JSON-based
- File-based
- Maintained outside the sink

The watermark is updated only after successful application of an SCN.

```java
if (result != null &&
    "SUCCESS".equalsIgnoreCase(result.getStatus())) {

    waterMark.save(datasetName, scn);
}
```

This guarantees:

- Ordered processing
- Idempotency
- Safe restart behavior

### 3.4 Pipeline Manifest JSON

Both source and sink need to be configured.

- `type` defines storage/backend
- `format` defines logical format
- `config` contains implementation-specific settings

Example:

```json
{
  "name": "fditcf_to_deltalake",
  "source": {
    "format": "fdi-tcf",
    "type": "OCI_OBJECT_STORAGE",
    "config": {
      "path": "oci://fdi_publish_data@axlekgviycu6/data/tcf/FUSION/"
    }
  },
  "sink": {
    "format": "delta_lake",
    "type": "OCI_OBJECT_STORAGE",
    "config": {
      "path": "oci://fdi-customer-pipeline@axlekgviycu6/data/delta/",
      "logRetentionDuration": "INTERVAL 30 DAYS"
    }
  },
  "watermark": {
    "type": "json",
    "path": "oci://fdi-customer-pipeline@axlekgviycu6/watermark"
  }
}
```

### 3.5 Source Interface

```java
public interface Source {

    void validate(PipelineManifest.SourceConfig sourceConfig) {}

    void initialize(SparkSession sparkSession);

    List<String> listDatasets();

    List<Long> listPendingSCNsAfter(
        String datasetName,
        long lastProcessedSCN
    );

    Dataset<Row> readChangeSets(
        String datasetName,
        long scn
    );
}
```

Pending SCNs are typically identified using:

```java
Dataset<Row> pendingScns = sparkSession.read()
                .format("fdi-tcf")
                .option("startingScn", lastProcessedScn + 1)
                .load(datasetRootPath)
                .select("fdi_scn_id")
                .distinct()
                .sort("fdi_scn_id");
```

This ensures:

- Distinct SCNs
- Ordered processing
- Incremental execution

### 3.6 Sink Interface and Extensibility

The `Sink` interface defines how changesets are written to a destination.

```java
public interface Sink {

    // Combination of type and format defines the Sink
    String type();

    // Returns the data format
    String format();

    // Validate sink configuration
    default void validate(
        PipelineManifest.SinkConfig sinkConfig
    ) {}

    // Initialize Spark session
    void initialize(SparkSession spark);

    // Apply changesets to sink
    WriteSummary applyChangeSets(
        String datasetName,
        Dataset<Row> changesets,
        DatasetSchema schema
    );
}
```

Any new sink can be added by implementing this interface.

Code:

`datashare-tcf-pipeline/src/main/java/com/oracle/fdi/datashare/tcf/sink/`

### 3.7 Full Refresh vs Incremental Processing

Each changeset (SCN) contains rows with a metadata column called:

```text
fdi_change_type
```

This identifies whether the SCN represents:

- A full refresh
- An incremental update

Typical logic:

```java
Dataset<Row> changeset = spark.read()
                           .format("fdi-tcf")
                           .option("startingScn", scn)
                           .option("endingScn", scn)
                           .load(path);

Row row = changeset.select("fdi_change_type").first();

boolean isFullRefresh =
    "full".equalsIgnoreCase(row.getString(0));
```

Processing strategy:

- Full refresh → rebuild target dataset
- Incremental → apply merge/update/delete logic

Because all rows within an SCN share the same `fdi_change_type`, only a minimal read is required to determine the processing mode.

### 3.8 Watermark File

Watermark tracks the latest applied SCN for each dataset.

Default implementation:

- JSON-based
- One file per dataset

Example:

```json
{
  "datasetName": "DW_DATASET_01",
  "lastAppliedSCN": "1762377422000",
  "updatedAt": "2026-02-20T18:45:30Z"
}
```

### 3.9 Pipeline History (Work in Progress)

Pipeline history provides summary information for every pipeline execution.

Example success response:

```json
{
  "pipeline_run_id": "<GUID>",
  "run_at": "2026-02-02T01:00:00Z",
  "status": "SUCCESS",
  "summary": [
    "20 datasets have been processed"
  ]
}
```

Example failure response:

```json
{
  "pipeline_run_id": "<GUID>",
  "run_at": "2026-02-02T01:00:00Z",
  "status": "FAILED",
  "summary": [
    "10 datasets have been processed",
    "Failed processing dataset DW_WEEK_D: Table not found or unauthorized"
  ]
}
```

## 4. Setup of TCF Data Pipeline in OCI DataFlow (OCI's Managed Spark Environment)

OCI Dataflow provides Spark environment to run such pipelines in the OCI world. This section explain in detail on how to setup our sample pipeline in OCI dataflow

To execute this Spark workload in OCI DataFlow:

1. Configure the OCI DataFlow application with the TCF Spark workload JAR.
2. Pass the pipeline manifest JSON file as an application argument.
3. Ensure the workload has access to the source and destination Object Storage locations.

### 4.1 OCI DataFlow App Setup

The DataFlow application should reference the TCF pipeline JAR as its executable.

- Executable file:
  `samples/tcf-data-extractor/target/tcf-data-extractor-1.0.0.jar`

- Main class name:
  `com.oracle.fdi.datashare.Application`

- Runtime requirement:
  Java 17

When launching the sample with Spark, use the main class explicitly. Example:

```text
spark-submit --class com.oracle.fdi.datashare.Application \
  samples/tcf-data-extractor/target/tcf-data-extractor-1.0.0.jar \
  -f oci://fdi-customer-pipeline@axlekgviycu6/ops/fditcf_to_deltalake.json
```

Pipeline manifest file can be passed as an argument to the Spark workload.

Example:

```text
-f oci://fdi-customer-pipeline@axlekgviycu6/ops/fditcf_to_deltalake.json
```

Required OCI policy:

```text
allow any-user to manage objects in compartment fdi-customer
where request.principal.type='dataflowrun'
```

The Spark workload requires access to:

- Source Object Storage locations containing TCF change sets
- Destination Object Storage locations where Delta Lake tables and watermarks are written

The pipeline manifest defines these source and destination locations and drives the runtime behavior of the Spark application.

### 4.2 What Happens During the Run

This workload runs based on the configured Pipeline Manifest.

Sample Pipeline Manifest:

```json
{
  "name": "sample_tcf_pipeline",
  "source": {
    "format": "fdi-tcf",
    "type": "OCI_OBJECT_STORAGE",
    "config": {
      "path": "oci://fdi_publish_data@axlekgviycu6/data/tcf/FUSION/"
    }
  },
  "sink": {
    "format": "delta_lake",
    "type": "OCI_OBJECT_STORAGE",
    "config": {
      "path": "oci://fdi-customer-pipeline@axlekgviycu6/data/delta/"
    }
  },
  "watermark": {
    "type": "json",
    "path": "oci://fdi-customer-pipeline@axlekgviycu6/watermark"
  }
}
```

At runtime, the pipeline performs the following steps:

#### Discover Datasets

The pipeline scans the configured source path and identifies all datasets available in TCF layout.

#### Determine Pending SCNs

For each dataset:

1. Reads the corresponding watermark state.
2. Determines the list of SCNs that have not yet been applied.
3. Sorts SCNs in ascending order.

Typical SCN discovery flow:

```java
Dataset<Row> pendingScns = sparkSession.read()
                .format("fdi-tcf")
                .option("startingScn", lastProcessedScn + 1)
                .load(datasetRootPath)
                .select("fdi_scn_id")
                .distinct()
                .sort("fdi_scn_id");
```

This ensures:

- Ordered processing
- Incremental execution
- Restartability
- Idempotency

#### Apply Change Sets Sequentially

For each pending SCN:

1. Read the SCN change set.
2. Read the associated schema.
3. Apply inserts/updates/deletes to the sink.
4. Update watermark after successful completion.

Typical processing flow:

```java
DatasetSchema schema =
    source.getSchema(datasetName, scn);

Dataset<Row> changeset =
    source.readChangeSets(datasetName, scn);

WriteSummary result =
    sink.applyChangeSets(
        datasetName,
        changeset,
        schema
    );

if (result != null &&
    "SUCCESS".equalsIgnoreCase(result.getStatus())) {

    waterMark.save(datasetName, scn);
}
```

Because SCNs are processed sequentially and watermarks advance only after successful completion, each SCN becomes:

- Atomic
- Restartable
- Deterministic

#### Full Refresh vs Incremental Processing

Each SCN contains metadata column:

```text
fdi_change_type
```

This identifies whether the changeset represents:

- Full refresh
- Incremental changes

Typical detection logic:

```java
Dataset<Row> changeset = spark.read()
                           .format("fdi-tcf")
                           .option("startingScn", scn)
                           .option("endingScn", scn)
                           .load(path);

Row row = changeset
            .select("fdi_change_type")
            .first();

boolean isFullRefresh =
    "full".equalsIgnoreCase(row.getString(0));
```

Processing behavior:

- Full refresh → rebuild target dataset
- Incremental → apply merge/update/delete operations

#### Watermark Storage and Layout

For a manifest passed as:

```text
oci://<bucket>/<manifestName>.json
```

watermarks for that pipeline are typically written under:

```text
oci://<bucket>/<manifestName>/watermark/
```

Sample watermark file:

```json
{
  "datasetName": "DW_DATASET_01",
  "lastAppliedSCN": "1762377422000",
  "updatedAt": "2026-02-20T18:45:30Z"
}
```

This watermark determines the latest successfully applied SCN for the dataset.

Subsequent runs process only newer SCNs.

## 5. TCF Reader Library - Interface Overview

In general, Spark readers exist for different popular formats like csv, json, parquet, delta, iceberg, etc.

```java
// Spark reader - for parquet format
Dataset<Row> df = spark.read()
                .format("parquet")
                .option("mergeSchema", "false")     // Avoid unless needed
                .option("basePath", "/data")        // Important for partitioned data
                .load("/data/year=2026/month=02");

// Spark reader - for csv format.
Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")           // First row as column names
                .option("inferSchema", "true")      // Infer data types
                .option("delimiter", ",")           // Custom delimiter
                .option("mode", "PERMISSIVE")       // Error handling
                .load("/data/input.csv");
```

Here also, we wanted to provide Spark industry standard interface for this format FDI's TCF and abstract the client from the details.

```java
// Spark reader - for FDI's TCF format.
Dataset<Row> changesets = sparkSession.read()
                .format("fdi-tcf")
                .option("startingScn", lastProcessedScn + 1)
                .option("endingScn", endingScn)
                .load(datasetRootPath);
```

What matters here is that once the data is returned as a `Dataset<Row>`, downstream Spark code can continue to use standard Spark semantics.

The goal of this interface is to make TCF data behave like any standard Spark datasource while hiding the underlying implementation details from the client.

SCN-bound reads are also supported.

When `startingScn` and `endingScn` are provided, the read is narrowed to that SCN window. When they are omitted, the datasource uses marker files under the dataset's `markers/` directory to determine the valid read range and reads from the latest full refresh SCN up to the latest SCN.

In other words, the datasource can work in two modes:

1. Default mode:
    - Read from the latest full refresh forward
2. Explicit window mode:
    - Read only the SCNs between `startingScn` and `endingScn`

This makes the interface suitable for both full refresh and incremental processing.

In this way:

- This source becomes part of Catalyst logical plan/execution plan.
- Works with Spark's optimizer rules.
- Works with Spark's AQE (Adaptive Query Execution)
- Is well integrated into Spark's ecosystem.
- Also, a functional / flow-style API is being provided and is in line with Spark's programming style.

## 6. FDI's TCF Reader Library

### 6.1 Abstraction of Typical Spark Reader for This Format

FDI's TCF format stores data in Parquet files organized by SCN folders and changeset folders.

Marker files exist to identify:

- The latest full refresh SCN
- The latest SCN
- Schema compatibility boundaries

Schema files are also associated with each SCN folder.

This reader library provides an abstraction of reading relevant and needed changesets while hiding the underlying file layout and metadata details from the client.

The result is a Spark-native experience that behaves like a standard datasource while still handling TCF-specific logic internally.

#### Planning Phase

- Data source resolution
- Logical plan and optimization:
    - Schema read/merge
    - Predicate pushdown
    - Column pruning
    - Filter reordering
- Physical planning

#### Execution Phase

- DAG creation and task scheduling
- File scan execution:
    - Partition pruning
    - Filter pushdown
- Shuffle and result collection

---

### 6.2 DataSource Registry

To give Spark's Reader interface for this new source, TCF must be recognized as a data source.

For that, this class is registered:

`com.oracle.fdi.spark.sql.fditcf.source.FdiTcfDataSource`

It is registered through:

`org.apache.spark.sql.sources.DataSourceRegister`

Reference:
`datashare-tcf-reader/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`

---

### 6.3 TCF DataSource and Associated Classes

- This DataSource class implements Spark's standard interfaces `TableProvider` and `DataSourceRegister`.
- `FdiTcfDataSource` builds on top of Spark's Parquet datasource implementation while adding TCF-specific SCN and schema handling.
- It abstracts TCF-specific marker resolution, schema handling, and SCN-based reads behind a standard Spark datasource interface.
- It provides an abstraction for reading changesets (inserts, deletes, updates) with these extra columns:
    1. `fdi_scn_id`
    2. `fdi_table_change_type`
    3. `fdi_change_type`
- `FdiTcfScanBuilder` handles SCN-based partition pruning and schema resolution during query planning.

If no `startingScn` and `endingScn` are provided, the datasource automatically resolves the valid SCN range using marker files.

```java
// Spark reader - for FDI's TCF format with no startingScn or endingScn.
// It reads changeset data from latestFullRefreshSCN to latestSCN
Dataset<Row> changesets = sparkSession.read()
                .format("fdi-tcf")
                .load(datasetRootPath);
```

If `startingScn` and/or `endingScn` are provided, it applies partition filtering and returns only the relevant SCNs to scan and read.

```java
// Spark reader - for FDI's TCF format with specified startingScn and endingScn.
// Within the changeset data from latestFullRefreshSCN to latestSCN, further filtering happens within that SCN window.
Dataset<Row> changesets = sparkSession.read()
                .format("fdi-tcf")
                .option("startingScn", lastProcessedScn + 1)
                .option("endingScn", endingScn)
                .load(datasetRootPath);
```
