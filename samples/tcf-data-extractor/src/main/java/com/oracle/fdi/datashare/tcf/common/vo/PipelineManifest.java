package com.oracle.fdi.datashare.tcf.common.vo;

import java.util.Map;

public class PipelineManifest {
    private String name;
    private SourceConfig source;
    private SinkConfig sink;
    private WatermarkConfig watermark;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public SourceConfig getSource() { return source; }
    public void setSource(SourceConfig source) { this.source = source; }

    public SinkConfig getSink() { return sink; }
    public void setSink(SinkConfig sink) { this.sink = sink; }

    public WatermarkConfig getWatermark() { return watermark; }
    public void setWatermark(WatermarkConfig watermark) { this.watermark = watermark; }

    /**
     * Source configuration: owns type/format and a generic config wrapper.
     */
    public static class SourceConfig {
        private String format;
        private String type;
        private Map<String, Object> config;

        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public Map<String, Object> getConfig() { return config; }
        public void setConfig(Map<String, Object> config) { this.config = config; }
    }

    /**
     * Sink configuration: owns type/format and a free-form config map.
     */
    public static class SinkConfig {
        private String format;
        private String type;
        private Map<String, Object> config;

        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public Map<String, Object> getConfig() { return config; }
        public void setConfig(Map<String, Object> config) { this.config = config; }
    }

    public static class WatermarkConfig {
        private String path;
        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
    }
}
