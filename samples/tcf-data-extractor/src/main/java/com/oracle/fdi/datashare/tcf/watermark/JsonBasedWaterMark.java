package com.oracle.fdi.datashare.tcf.watermark;

import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import com.oracle.fdi.datashare.tcf.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonBasedWaterMark implements WaterMark {

    private static Logger log = LoggerFactory.getLogger(JsonBasedWaterMark.class);

    private final SparkSession sparkSession;
    private final Path watermarkDir;

    public static class DatasetWatermark {
        public String datasetName;
        public Long lastAppliedSCN;
        public long updatedAt;
    }

    public JsonBasedWaterMark(SparkSession sparkSession, Path watermarkDir) {
        this.sparkSession = sparkSession;
        this.watermarkDir = watermarkDir;
    }

    @Override
    public Optional<Long> getLastAppliedSCN(String datasetName) {
        Path wmFile = new Path(watermarkDir, datasetName + ".json");
        if (!Utils.exists(sparkSession, wmFile)) {
            return Optional.empty();
        }
        DatasetWatermark wm = Utils.jsonToObj(sparkSession, wmFile, DatasetWatermark.class);
        if (wm == null || wm.lastAppliedSCN == null) {
            return Optional.empty();
        }
        return Optional.of(wm.lastAppliedSCN);
    }

    @Override
    public void save(String datasetName, long scn) {
        DatasetWatermark wm = new DatasetWatermark();
        wm.datasetName = datasetName;
        wm.lastAppliedSCN = scn;
        wm.updatedAt = System.currentTimeMillis();

        // Ensure watermark dir exists
        Utils.createDirIfNotExist(sparkSession, watermarkDir);

        Path wmFile = new Path(watermarkDir, datasetName + ".json");
        log.info("WATERMARK DIR  = {}", watermarkDir.toString());
        log.info("WATERMARK FILE = {}", wmFile.toString());
        Utils.writeObjAsJson(sparkSession, wmFile, wm, true);
        log.info("Dataset {} : watermark set to {}", datasetName, scn);
    }
}
