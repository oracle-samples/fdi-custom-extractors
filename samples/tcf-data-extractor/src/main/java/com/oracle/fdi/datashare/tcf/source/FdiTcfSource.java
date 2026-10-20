package com.oracle.fdi.datashare.tcf.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.oracle.fdi.datashare.tcf.common.ScnId;
import com.oracle.fdi.datashare.tcf.common.Utils;
import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;
import com.oracle.fdi.spark.sql.fditcf.source.FdiTcfDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.oracle.fdi.datashare.tcf.sdk.api.ApiConstants.*;

public class FdiTcfSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(FdiTcfSource.class);

    private final PipelineManifest.SourceConfig sourceConfig;
    private SparkSession sparkSession;
    private Path hdfsBaseFilePath;

    public FdiTcfSource(PipelineManifest.SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void initialize(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.hdfsBaseFilePath = new Path(sourceConfig.getConfig().get("path").toString());
    }

    @Override
    public List<String> listDatasets() {
        List<String> datasets = Utils.listFiles(hdfsBaseFilePath, sparkSession, true);
        log.info("Datasets under {}: {}", hdfsBaseFilePath, datasets);
        return datasets;
    }

    @Override
    public void validate(PipelineManifest.SourceConfig sourceConfig) {
        Utils.requireNonNull(sourceConfig.getConfig(), "source.config");
        Utils.requireNonNull(sourceConfig.getConfig().get("path"), "source.config.path");
        Utils.requireNonBlank(sourceConfig.getConfig().get("path").toString(), "source.config.path");
    }

    /**
     * Lists completed SCNs greater than lastProcessedScn without reading data rows.
     */
    @Override
    public List<Long> listPendingScnIdsAfter(String datasetName, long lastProcessedScn) {
        if (sparkSession == null) {
            throw new IllegalStateException("Source not initialized: sparkSession is null");
        }

        String startingScn = Long.toString(lastProcessedScn + 1L);
        Path datasetRoot = new Path(hdfsBaseFilePath, datasetName);

        List<String> folders = Utils.listFiles(datasetRoot, sparkSession, true);
        boolean hasCompletedScn = folders.stream()
                .filter(name -> name.startsWith(FDI_SCN_ID + "="))
                .anyMatch(name -> Utils.exists(sparkSession, new Path(new Path(datasetRoot, name), "_DONE")));
        if (!hasCompletedScn) {
            return List.of();
        }

        List<Long> availableScns = FdiTcfDataSource.listScnPaths(datasetRoot, sparkSession).stream()
                .map(path -> ScnId.fromChangeId(path.getName()).value())
                .filter(scn -> scn > lastProcessedScn)
                .sorted()
                .collect(Collectors.toList());

        List<Long> scns = new ArrayList<>();
        for (Long scn : availableScns) {
            Path scnPath = new Path(datasetRoot, FDI_SCN_ID + "=" + scn);
            if (!Utils.exists(sparkSession, new Path(scnPath, "_DONE"))) {
                // Do not advance the watermark past an unfinished publish.
                break;
            }
            scns.add(scn);
        }

        log.info("Pending SCNs: dataset={} lastProcessedScn={} startingScn={} count={}",
                datasetName, lastProcessedScn, startingScn, scns.size());
        return scns;
    }

    @Override
    public Dataset<Row> readChangeSets(String datasetName, long scnId) {
        if (sparkSession == null) {
            throw new IllegalStateException("Source not initialized: sparkSession is null");
        }

        Path datasetRoot = new Path(hdfsBaseFilePath, datasetName);
        Path scnPath = new Path(datasetRoot, FDI_SCN_ID + "=" + scnId);
        if (!Utils.exists(sparkSession, new Path(scnPath, "_DONE"))) {
            throw new IllegalStateException("Publish is not complete: " + scnPath);
        }

        if (!hasDataFiles(scnPath)) {
            log.info("No data files for completed publish: dataset={} scn={}", datasetName, scnId);
            // The pipeline passes _schema.json separately so an empty full refresh can still
            // clear an existing target. Do not guess business-column types from Oracle metadata.
            StructType schema = new StructType()
                    .add(FDI_SCN_ID, DataTypes.LongType)
                    .add(FDI_TABLE_CHANGE_TYPE, DataTypes.StringType)
                    .add(FDI_CHANGE_TYPE, DataTypes.StringType);
            return sparkSession.createDataFrame(List.<Row>of(), schema);
        }

        Dataset<Row> changesets = sparkSession.read()
                .format("fdi-tcf")
                .option("startingScn", String.valueOf(scnId))
                .option("endingScn", String.valueOf(scnId))
                .load(datasetRoot.toString());

        return changesets;
    }

    private boolean hasDataFiles(Path path) {
        try {
            FileSystem fs = path.getFileSystem(sparkSession.sparkContext().hadoopConfiguration());
            for (FileStatus status : fs.listStatus(path)) {
                String name = status.getPath().getName();
                if (name.startsWith("_") || name.startsWith(".")) {
                    continue;
                }
                if (status.isDirectory() && hasDataFiles(status.getPath())) {
                    return true;
                }
                // Let Spark validate every visible file, including files without a .parquet
                // suffix. A corrupt or unexpected file must not become an empty full refresh.
                if (status.isFile()) {
                    return true;
                }
            }
            return false;
        } catch (IOException ex) {
            throw new RuntimeException("Failed to inspect Parquet files under " + path, ex);
        }
    }

    @Override
    public DatasetSchema getSchema(String datasetName, long scnId) {
        Path datasetPath = new Path(hdfsBaseFilePath, datasetName);
        Path scnFolderPath = new Path(datasetPath, "fdi_scn_id=" + scnId);
        Path schemaFile = new Path(scnFolderPath, "_schema.json");

        DatasetSchema schema = Utils.jsonToObj(sparkSession, schemaFile, DatasetSchema.class);
        if (schema == null) {
            throw new RuntimeException("Schema file missing or invalid: " + schemaFile);
        }

        return schema;
    }

    @Override
    public String toString() {
        return String.format("FDI TCF Source with basePath: %s", hdfsBaseFilePath);
    }
}
