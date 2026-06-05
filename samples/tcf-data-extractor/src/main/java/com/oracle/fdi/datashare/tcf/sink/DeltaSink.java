package com.oracle.fdi.datashare.tcf.sink;

import java.util.stream.Collectors;
import java.util.List;

import org.apache.hadoop.fs.Path;
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
    private static final String SOURCE_FDI_CHANGE_TYPE = SOURCE_ALIAS + "." + FDI_CHANGE_TYPE;

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
    public WriteSummary applyChangeSets(String datasetName, Dataset changesets, DatasetSchema schema) {
        // Target Delta table location for this dataset
         Path deltaTablePath = new Path(deltaBasePath, datasetName);

        boolean isFullRefresh = false;
        List<Row> rows = changesets.limit(1).collectAsList();
        if (!rows.isEmpty()) {
            if (FDI_CHANGE_TYPE_FULL_REFRESH.equalsIgnoreCase(rows.get(0).getAs(FDI_TABLE_CHANGE_TYPE))) {
                isFullRefresh = true;
            }
        }

        // Bootstrap the Delta table if it does not exist
        if (isFullRefresh) {
            log.info("Bootstrapping full refresh for {} at {}", datasetName, deltaTablePath);

            SparkSession spark = changesets.sparkSession();
            String path = deltaTablePath.toString();

            Dataset<Row> bootstrapDf = changesets.drop(FDI_TABLE_CHANGE_TYPE, FDI_CHANGE_TYPE, FDI_SCN_ID);

            if (DeltaTable.isDeltaTable(spark, path)) {
                // remove existing rows without writing an empty parquet file
                DeltaTable.forPath(spark, path).delete(expr("true"));
            } else {
                // create metadata-only delta table using the incoming DataFrame schema
                DeltaTable.createIfNotExists(spark)
                        .location(path)
                        .addColumns(bootstrapDf.schema())
                        .execute();
            }
        }

        // Open the target Delta table and build the primary-key merge predicate.
        DeltaTable targetDeltaTable = DeltaTable.forPath(changesets.sparkSession(), deltaTablePath.toString());
        String mergeCondition = buildMergeCondition(schema);

        // For efficiency: only dedupe for non-full refresh batches.
        Dataset<Row> sourceForMerge;
        if (isFullRefresh) {
            sourceForMerge = changesets;
        } else {
            sourceForMerge = SinkUtils.dedupeChangesets(changesets, schema);
        }

        targetDeltaTable.as(TARGET_ALIAS)
                .merge(sourceForMerge.as(SOURCE_ALIAS), mergeCondition)
                .whenMatched(col(SOURCE_FDI_CHANGE_TYPE).equalTo(lit(FDI_CHANGE_TYPE_DELETE))).delete()
                .whenMatched().updateAll()
                .whenNotMatched(col(SOURCE_FDI_CHANGE_TYPE).notEqual(lit(FDI_CHANGE_TYPE_DELETE))).insertAll()
                .execute();

        WriteSummary summary = new WriteSummary();
        summary.setStatus("SUCCESS");
        return summary;
    }

    private String buildMergeCondition(DatasetSchema schema) {
        List<String> pkCols = schema.getPrimaryKeyList();
        if (pkCols == null || pkCols.isEmpty()) {
            throw new IllegalArgumentException("Primary key columns are empty; cannot build merge condition");
        }

        return pkCols.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(pk -> SOURCE_ALIAS + ".`" + pk + "` = " + TARGET_ALIAS + ".`" + pk + "`")
                .collect(Collectors.joining(" and "));
    }

    @Override
    public String toString() {
        return "Delta Sink with path : " + deltaBasePath;
    }
}
