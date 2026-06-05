package com.oracle.fdi.datashare.tcf.common.vo;

import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;

public class DatasetSchema {

    private String tableName;
    private Boolean fullRefresh;
    private String primaryKeyCols;
    private List<Column> schema;


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Boolean getFullRefresh() {
        return fullRefresh;
    }

    public void setFullRefresh(Boolean fullRefresh) {
        this.fullRefresh = fullRefresh;
    }

    public String getPrimaryKeyCols() {
        return primaryKeyCols;
    }

    public void setPrimaryKeyCols(String primaryKeyCols) {
        this.primaryKeyCols = primaryKeyCols;
    }

    public List<Column> getSchema() {
        return schema;
    }

    public void setSchema(List<Column> schema) {
        this.schema = schema;
    }

    public List<String> getPrimaryKeyList() {
        if (primaryKeyCols == null || primaryKeyCols.isBlank()) {
            return List.of();
        }
        return Arrays.stream(primaryKeyCols.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    public static class Column {
        private String colName;
        private String description;
        private Boolean mandatory;
        private String datatype;
        private Integer length;

        public String getColName() { return colName; }
        public void setColName(String colName) { this.colName = colName; }

        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }

        public Boolean getMandatory() { return mandatory; }
        public void setMandatory(Boolean mandatory) { this.mandatory = mandatory; }

        public String getDatatype() { return datatype; }
        public void setDatatype(String datatype) { this.datatype = datatype; }

        public Integer getLength() { return length; }
        public void setLength(Integer length) { this.length = length; }
    }
}
