package com.oracle.fdi.datashare.tcf.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;
import com.oracle.fdi.datashare.tcf.common.vo.WriteSummary;

/**
 * This represents a sink/destination for the data.
 */
public interface Sink {
    String type();
    String format();

    WriteSummary applyChangeSets(String datasetName, Dataset<Row> changesets, DatasetSchema schema);

    void initialize(SparkSession spark);

    default void validate(PipelineManifest.SinkConfig sinkConfig) {}
}
