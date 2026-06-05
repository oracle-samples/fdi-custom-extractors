package com.oracle.fdi.datashare;

import org.apache.spark.sql.SparkSession;
import com.oracle.fdi.datashare.tcf.pipeline.TcfPipeline;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        log.info("Starting TCF pipeline application");

        Options options = new Options();
        options.addOption("f", "pipelineManifestFile", true, "Pipeline Manifest File");
        options.addOption("h", "help", false, "Show help");

        CommandLine cmd;
        String pipelineManifestFilePath;

        try {
            cmd = new DefaultParser().parse(options, args);

            if (cmd.hasOption("help") || !cmd.hasOption("pipelineManifestFile")) {
                showHelp(options);
                System.exit(1);
                return;
            }

            pipelineManifestFilePath = cmd.getParsedOptionValue("pipelineManifestFile").toString();

        } catch (ParseException e) {
            showHelp(options);
            System.exit(1);
            return;
        }

        SparkSession sparkSession = SparkSession.builder()
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .appName("TcfPipelineApp")
                .getOrCreate();

        log.info("Pipeline manifest: {}", pipelineManifestFilePath);
        TcfPipeline.build(pipelineManifestFilePath, sparkSession).run();
        log.info("TCF pipeline application finished");
    }

    private static void showHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("spark-submit app.jar", options);
    }
}
