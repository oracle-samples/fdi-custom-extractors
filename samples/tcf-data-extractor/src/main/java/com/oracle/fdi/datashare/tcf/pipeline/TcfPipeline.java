package com.oracle.fdi.datashare.tcf.pipeline;

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.oracle.fdi.datashare.tcf.common.Utils;
import com.oracle.fdi.datashare.tcf.common.TcfPipelineException;
import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;
import com.oracle.fdi.datashare.tcf.common.vo.WriteSummary;
import com.oracle.fdi.datashare.tcf.sink.Sink;
import com.oracle.fdi.datashare.tcf.sink.SinkFactory;
import com.oracle.fdi.datashare.tcf.source.Source;
import com.oracle.fdi.datashare.tcf.source.SourceFactory;
import com.oracle.fdi.datashare.tcf.watermark.WaterMark;
import com.oracle.fdi.datashare.tcf.watermark.JsonBasedWaterMark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcfPipeline {

    private static final Logger log = LoggerFactory.getLogger(TcfPipeline.class);

    private Sink sink;
    private Source source;
    private PipelineManifest pipelineManifest;
    private WaterMark waterMark;

    public static TcfPipeline build(String pipelineManifestPath, SparkSession sparkSession) {
        TcfPipeline pipeline = new TcfPipeline();
        pipeline.initialize(pipelineManifestPath, sparkSession);
        return pipeline;
    }

    public void initialize(String pipelineManifestPath, SparkSession sparkSession) {
        Utils.requireNonBlank(pipelineManifestPath, "pipelineManifestPath");
        log.info("Manifest file path: {}", pipelineManifestPath);

        Path manifestPath = new Path(pipelineManifestPath);
        this.pipelineManifest = Utils.jsonToObj(sparkSession, manifestPath, PipelineManifest.class);

        if (pipelineManifest == null) {
            throw new TcfPipelineException("Invalid Pipeline Manifest file " + pipelineManifestPath);
        }

        log.info("Manifest: {}", Utils.toJson(pipelineManifest));

        // Validate manifest structure and required fields
        validate();

        // Use watermark.path from manifest
        Path wmDir = new Path(pipelineManifest.getWatermark().getPath());
        this.waterMark = new JsonBasedWaterMark(sparkSession, wmDir);
        log.info("WaterMark initialized: {}", wmDir);

        // Initialize Source from manifest
        this.source = SourceFactory.getSource(pipelineManifest.getSource());
        source.validate(pipelineManifest.getSource());
        source.initialize(sparkSession);
        log.info("Source initialized: {}", source);

        // Initialize Sink from manifest
        this.sink = SinkFactory.getSink(pipelineManifest.getSink());
        sink.validate(pipelineManifest.getSink());
        sink.initialize(sparkSession);
        log.info("Sink initialized: {}", sink);
    }

    private void validate() {
        Utils.requireNonNull(pipelineManifest, "manifest");
        Utils.requireNonBlank(pipelineManifest.getName(), "manifest.name");

        Utils.requireNonNull(pipelineManifest.getSource(), "source");
        Utils.requireNonNull(pipelineManifest.getSink(), "sink");

        // Source: format and type are required
        Utils.requireNonNull(pipelineManifest.getSource().getFormat(), "source.format");
        Utils.requireNonNull(pipelineManifest.getSource().getType(),   "source.type");
        Utils.requireNonBlank(pipelineManifest.getSource().getFormat(), "source.format");
        Utils.requireNonBlank(pipelineManifest.getSource().getType(),   "source.type");

        // Sink: format and type are required
        Utils.requireNonNull(pipelineManifest.getSink().getFormat(), "sink.format");
        Utils.requireNonNull(pipelineManifest.getSink().getType(),   "sink.type");
        Utils.requireNonBlank(pipelineManifest.getSink().getFormat(), "sink.format");
        Utils.requireNonBlank(pipelineManifest.getSink().getType(),   "sink.type");

        // Watermark: required, with non-blank path
        Utils.requireNonNull(pipelineManifest.getWatermark(), "watermark");
        Utils.requireNonBlank(pipelineManifest.getWatermark().getPath(), "watermark.path");
    }


    public void run() {
        try {
            log.info("Pipeline run started");

            source.listDatasets().forEach(datasetName -> {
                Optional<Long> lastAppliedOpt = waterMark.getLastAppliedSCN(datasetName);
                log.info("Dataset: {}, Last Applied SCN: {}", datasetName, lastAppliedOpt);

                long lastAppliedScn = lastAppliedOpt.orElse(0L);

                List<Long> pendingScns = source.listPendingScnIdsAfter(datasetName, lastAppliedScn);
                log.info("Pending SCNs for {}: {}", datasetName, pendingScns);

                pendingScns.forEach(scn -> {
                    log.info("Processing dataset={} scn={}", datasetName, scn);

                    DatasetSchema schema = source.getSchema(datasetName, scn);
                    Dataset changesets = source.readChangeSets(datasetName, scn);

                    WriteSummary writeSummary = sink.applyChangeSets(datasetName, changesets, schema);
                    if (writeSummary == null || writeSummary.getStatus() == null || !"SUCCESS".equalsIgnoreCase(writeSummary.getStatus())) {
                        throw new TcfPipelineException(String.format("Write failed for dataset=%s scn=%s status=%s", datasetName, scn, writeSummary.getStatus()));
                    }

                    waterMark.save(datasetName, scn);
                    log.info("Completed dataset={} scn={}", datasetName, scn);
                });
            });

            log.info("Pipeline run completed successfully");
        }
        catch (Exception ex) {
            log.error("Pipeline run failed", ex);
            throw new TcfPipelineException("Pipeline run failed", ex);
        }
    }
}
