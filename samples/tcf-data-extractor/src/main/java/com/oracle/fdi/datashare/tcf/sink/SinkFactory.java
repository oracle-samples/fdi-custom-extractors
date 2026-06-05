package com.oracle.fdi.datashare.tcf.sink;

import org.apache.commons.lang3.StringUtils;

import com.oracle.fdi.datashare.tcf.common.TcfPipelineException;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;

public class SinkFactory {

    /**
     * Create a Sink based on (sink.type, sink.format).
     * For now we only support Delta sink for:
     *   - sink.format = "delta_lake"
     *   - sink.type   = "LOCAL_FILESYSTEM" or "OCI_OBJECT_STORAGE"
     */
    public static Sink getSink(PipelineManifest.SinkConfig sinkConfig) {

        String format = StringUtils.trim(sinkConfig.getFormat());
        String type = StringUtils.trim(sinkConfig.getType());

        if ("delta_lake".equalsIgnoreCase(format) && ("LOCAL_FILESYSTEM".equalsIgnoreCase(type) || "OCI_OBJECT_STORAGE".equalsIgnoreCase(type))) {
            return new DeltaSink(sinkConfig);
        }

        throw new TcfPipelineException("No sink has been defined for sink.type=" + type + ", sink.format=" + format);
    }
}
