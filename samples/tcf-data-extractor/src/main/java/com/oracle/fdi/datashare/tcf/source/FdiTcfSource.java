package com.oracle.fdi.datashare.tcf.source;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.oracle.fdi.datashare.tcf.common.Utils;
import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Reads list of SCNs greater than lastProcessedScn.
     */
    @Override
    public List<Long> listPendingScnIdsAfter(String datasetName, long lastProcessedScn) {
        if (sparkSession == null) {
            throw new IllegalStateException("Source not initialized: sparkSession is null");
        }

        String startingScn = Long.toString(lastProcessedScn + 1L);
        Path datasetRoot = new Path(hdfsBaseFilePath, datasetName);

        Dataset<Row> scnOnly = sparkSession.read()
                .format("fdi-tcf")
                .option("startingScn", startingScn)
                .load(datasetRoot.toString())
                .select("fdi_scn_id")
                .distinct()
                .sort("fdi_scn_id");

        List<Long> scns = scnOnly.collectAsList().stream()
                .map(r -> r.getLong(0))
                .collect(Collectors.toList());

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

        Dataset<Row> changesets = sparkSession.read()
                .format("fdi-tcf")
                .option("startingScn", String.valueOf(scnId))
                .option("endingScn", String.valueOf(scnId))
                .load(datasetRoot.toString());

        return changesets;
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
