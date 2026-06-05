package com.oracle.fdi.datashare.tcf.source;

import org.apache.commons.lang3.StringUtils;

import com.oracle.fdi.datashare.tcf.common.TcfPipelineException;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;

public class SourceFactory {

    /**
     * Create a Source based on (source.type, source.format).
     * For now we only support the TCF source for:
     *   - source.format = "fdi-tcf"
     *   - source.type   = "LOCAL_FILESYSTEM" or "OCI_OBJECT_STORAGE"
     */
    public static Source getSource(PipelineManifest.SourceConfig sourceConfig) {
        if (sourceConfig == null) {
            throw new IllegalArgumentException("Source config is empty or not supported");
        }

        String format = StringUtils.trim(sourceConfig.getFormat());
        String type   = StringUtils.trim(sourceConfig.getType());

        if ("fdi-tcf".equalsIgnoreCase(format) && ("LOCAL_FILESYSTEM".equalsIgnoreCase(type) || "OCI_OBJECT_STORAGE".equalsIgnoreCase(type))) {
            return new FdiTcfSource(sourceConfig);
        }

        throw new TcfPipelineException("No source has been defined for source.type=" + type + ", source.format=" + format);
    }
}
