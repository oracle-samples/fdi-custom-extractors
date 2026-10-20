package com.oracle.fdi.datashare.tcf.sink;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oracle.fdi.datashare.tcf.common.Utils;
import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;
import com.oracle.fdi.datashare.tcf.common.vo.WriteSummary;

import io.delta.tables.DeltaTable;

import static com.oracle.fdi.datashare.tcf.sdk.api.ApiConstants.*;
import static org.apache.spark.sql.functions.*;

/**
 * Delta Lake sink implementation.
 */
public class DeltaSink implements Sink {

    private static final Logger log = LoggerFactory.getLogger(DeltaSink.class);

    private static final String SOURCE_ALIAS = "source";
    private static final String TARGET_ALIAS = "target";

    // Underlying sink configuration from manifest
    private final PipelineManifest.SinkConfig sinkConfig;

    private String deltaBasePath;

    public DeltaSink(PipelineManifest.SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
    }

    @Override
    public String type() {
        // Manifest sink.type (e.g. LOCAL_FILESYSTEM, OCI_OBJECT_STORAGE)
        return sinkConfig.getType();
    }

    @Override
    public String format() {
        // Return manifest sink.format instead of hardcoding
        return sinkConfig.getFormat();
    }

    @Override
    public void initialize(SparkSession spark) {
        // Spark 3.x canonical calendar behavior
        spark.conf().set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED");
        spark.conf().set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED");
        deltaBasePath = sinkConfig.getConfig().get("path").toString().trim();
    }

    @Override
    public void validate(PipelineManifest.SinkConfig sinkConfig) {
        Utils.requireNonNull(sinkConfig, "sink");
        Utils.requireNonNull(sinkConfig.getConfig(), "sink.config");
        Utils.requireNonNull(sinkConfig.getConfig().get("path"), "sink.config.path");
        Utils.requireNonBlank(sinkConfig.getConfig().get("path").toString(), "sink.config.path");
    }

    @Override
    public WriteSummary applyChangeSets(String datasetName, Dataset<Row> changesets, DatasetSchema schema) {
        // Target Delta table location for this dataset
        Path deltaTablePath = new Path(deltaBasePath, datasetName);

        List<Row> rows = changesets.limit(1).collectAsList();
        if (rows.isEmpty() && schema.getFullRefresh() == null) {
            throw new IllegalArgumentException("fullRefresh metadata is required for an empty publish: " + datasetName);
        }
        boolean isFullRefresh = Boolean.TRUE.equals(schema.getFullRefresh());
        if (!rows.isEmpty()) {
            isFullRefresh = FDI_CHANGE_TYPE_FULL_REFRESH.equalsIgnoreCase(rows.get(0).getAs(FDI_TABLE_CHANGE_TYPE));
        }

        SparkSession spark = changesets.sparkSession();
        String path = deltaTablePath.toString();
        boolean targetExists = DeltaTable.isDeltaTable(spark, path);

        if (isFullRefresh) {
            log.info("Bootstrapping full refresh for {} at {}", datasetName, deltaTablePath);
            if (targetExists) {
                // remove existing rows without writing an empty parquet file
                DeltaTable.forPath(spark, path).delete(expr("true"));
            }
        }

        // A metadata-only first full refresh has no physical schema. Defer creating its target
        // until Parquet provides actual types and column names, even if that file has zero rows.
        if (!targetExists) {
            Dataset<Row> bootstrapDf = changesets.drop(FDI_TABLE_CHANGE_TYPE, FDI_CHANGE_TYPE, FDI_SCN_ID);
            if (bootstrapDf.columns().length > 0) {
                DeltaTable.createIfNotExists(spark)
                        .location(path)
                        .addColumns(bootstrapDf.schema())
                        .execute();
            }
        }

        if (rows.isEmpty()) {
            WriteSummary summary = new WriteSummary();
            summary.setStatus("SUCCESS");
            return summary;
        }

        // Apply explicit deletes separately because date-effective delete rows may not contain the
        // primary key or the full table schema.
        DeltaTable targetDeltaTable = DeltaTable.forPath(changesets.sparkSession(), deltaTablePath.toString());
        Dataset<Row> deletes = changesets
                .filter(lower(col(FDI_CHANGE_TYPE)).equalTo(lit(FDI_CHANGE_TYPE_DELETE)));
        if (!deletes.isEmpty()) {
            List<String> deleteColumns = SinkUtils.getDeleteColumns(schema, deletes.columns());
            String deleteCondition = buildMergeCondition(deleteColumns);
            Column[] deleteColumnExpressions = deleteColumns.stream()
                    .map(column -> col(column))
                    .toArray(Column[]::new);
            log.info("Deleting rows from {} using columns {}", datasetName, deleteColumns);
            targetDeltaTable.as(TARGET_ALIAS)
                    .merge(deletes.select(deleteColumnExpressions).as(SOURCE_ALIAS), deleteCondition)
                    .whenMatched()
                    .delete()
                    .execute();
        }

        Dataset<Row> upserts = changesets
                .filter(lower(col(FDI_CHANGE_TYPE)).notEqual(lit(FDI_CHANGE_TYPE_DELETE)));
        if (!upserts.isEmpty()) {
            String mergeCondition = buildMergeCondition(
                    SinkUtils.resolveColumnNames(schema.getPrimaryKeyList(), upserts.columns()));

            // For efficiency: only dedupe for non-full refresh batches.
            Dataset<Row> sourceForMerge = isFullRefresh
                    ? upserts
                    : SinkUtils.dedupeChangesets(upserts, schema);

            targetDeltaTable.as(TARGET_ALIAS)
                    .merge(sourceForMerge.as(SOURCE_ALIAS), mergeCondition)
                    .whenMatched()
                    .updateAll()
                    .whenNotMatched()
                    .insertAll()
                    .execute();
        }

        WriteSummary summary = new WriteSummary();
        summary.setStatus("SUCCESS");
        return summary;
    }

    private String buildMergeCondition(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Merge columns are empty; cannot build merge condition");
        }

        return columns.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(column -> SOURCE_ALIAS + ".`" + column + "` = " + TARGET_ALIAS + ".`" + column + "`")
                .collect(Collectors.joining(" and "));
    }

    @Override
    public String toString() {
        return "Delta Sink with path : " + deltaBasePath;
    }
}
