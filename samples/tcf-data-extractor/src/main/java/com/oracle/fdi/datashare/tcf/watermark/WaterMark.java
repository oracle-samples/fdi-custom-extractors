package com.oracle.fdi.datashare.tcf.watermark;

import java.util.Optional;

public interface WaterMark {
    Optional<Long> getLastAppliedSCN(String datasetName);
    void save(String datasetName, long scn);
}
