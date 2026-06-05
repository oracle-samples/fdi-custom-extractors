package com.oracle.fdi.datashare.tcf.source;

import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Source {

    void initialize(SparkSession sparkSession);

    List<String> listDatasets();

    List<Long> listPendingScnIdsAfter(String datasetName, long lastProcessedScn);

    Dataset<Row> readChangeSets(String datasetName, long scnId);

    void validate(PipelineManifest.SourceConfig sourceConfig);

    DatasetSchema getSchema(String datasetName, long scnId);
}