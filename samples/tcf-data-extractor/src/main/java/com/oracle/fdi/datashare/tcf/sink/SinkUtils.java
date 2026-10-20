package com.oracle.fdi.datashare.tcf.sink;

import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.oracle.fdi.datashare.tcf.sdk.api.ApiConstants.*;
import static org.apache.spark.sql.functions.*;

/**
 * Common utilities for sink operations.
 */
public final class SinkUtils {

    private static final Logger log = LoggerFactory.getLogger(SinkUtils.class);

    private static final Random RANDOM = new Random();

    private SinkUtils() {}

    /**
     * Prefer an existing metadata name. Resolve a missing CX_ name only when its unprefixed
     * column exists in the published data; never rename the data or add a missing prefix.
     */
    public static List<String> resolveColumnNames(List<String> columnNames, String[] datasetColumns) {
        Map<String, String> availableColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String column : datasetColumns) {
            availableColumns.put(column, column);
        }

        return columnNames.stream()
                .map(column -> !availableColumns.containsKey(column)
                        && column.regionMatches(true, 0, "CX_", 0, 3)
                        ? availableColumns.getOrDefault(column.substring(3), column)
                        : column)
                .collect(Collectors.toList());
    }

    /**
     * Resolves the columns used for explicit delete rows. Date-effective columns take precedence
     * when the complete configured key is available, followed by the primary key and then the
     * available dataset columns from the published schema.
     */
    public static List<String> getDeleteColumns(DatasetSchema schema, String[] datasetColumns) {
        List<String> dateEffectiveColumns = getCompleteConfiguredColumns(
                schema.getDateEffectiveColumnList(), datasetColumns, schema);
        if (!dateEffectiveColumns.isEmpty()) {
            log.info("Using date-effective columns for deletes: {}", dateEffectiveColumns);
            return dateEffectiveColumns;
        }

        List<String> primaryKeyColumns = getCompleteConfiguredColumns(
                schema.getPrimaryKeyList(), datasetColumns, schema);
        if (!primaryKeyColumns.isEmpty()) {
            log.info("Using primary-key columns for deletes: {}", primaryKeyColumns);
            return primaryKeyColumns;
        }

        Set<String> availableColumns = caseInsensitiveSet(List.of(datasetColumns));
        List<String> schemaColumns = getSchemaColumnNames(schema, datasetColumns).stream()
                .filter(availableColumns::contains)
                .collect(Collectors.toList());
        if (schemaColumns.isEmpty()) {
            throw new IllegalArgumentException("No suitable columns are available for deletes");
        }

        log.info("Using all available dataset columns for deletes: {}", schemaColumns);
        return schemaColumns;
    }

    private static List<String> getCompleteConfiguredColumns(
            List<String> configuredColumns,
            String[] datasetColumns,
            DatasetSchema schema) {
        if (configuredColumns.isEmpty() || schema.getSchema() == null) {
            return List.of();
        }

        List<String> resolvedColumns = resolveColumnNames(configuredColumns, datasetColumns);
        Set<String> availableColumns = caseInsensitiveSet(List.of(datasetColumns));
        if (!resolvedColumns.stream().allMatch(availableColumns::contains)) {
            return List.of();
        }

        Set<String> configuredColumnSet = caseInsensitiveSet(resolvedColumns);
        List<String> matchingColumns = getSchemaColumnNames(schema, datasetColumns).stream()
                .filter(configuredColumnSet::contains)
                .collect(Collectors.toList());

        return matchingColumns.size() == configuredColumns.size() ? matchingColumns : List.of();
    }

    private static List<String> getSchemaColumnNames(DatasetSchema schema, String[] datasetColumns) {
        return schema.getSchema() == null
                ? List.of()
                : resolveColumnNames(schema.getSchema().stream()
                        .map(DatasetSchema.Column::getColName)
                        .collect(Collectors.toList()), datasetColumns);
    }

    private static Set<String> caseInsensitiveSet(List<String> columns) {
        Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.addAll(columns);
        return result;
    }

    /**
     * Deduplicate incoming change rows by primary key with preference order:
     * update (3) > insert (2) > delete (1). Assumes single-SCN batches,
     * so SCN is not used as a tie-breaker.
     */
    public static Dataset<Row> dedupeChangesets(Dataset<Row> changesets, DatasetSchema schema) {
        List<String> pkCols = resolveColumnNames(schema.getPrimaryKeyList(), changesets.columns());
        if (pkCols == null || pkCols.isEmpty()) {
            //Log an error message and return the original dataset
            log.error("Primary key columns are empty; cannot dedupe changesets");
            return changesets;
        }

        int randomNumber = RANDOM.nextInt(10000);
        String changePriorityColumn = "__fditcf_" + randomNumber + "__change_priority";
        String rowNumberColumn = "__fditcf_" + randomNumber + "__row_num";

        Column[] pkColumns = pkCols.stream().map(functions::col).toArray(Column[]::new);
        WindowSpec w = Window.partitionBy(pkColumns)
                .orderBy(col(changePriorityColumn).desc());

        Dataset<Row> withPriority = changesets
                .withColumn(
                        changePriorityColumn,
                        when(lower(col(FDI_CHANGE_TYPE)).equalTo(lit(FDI_CHANGE_TYPE_UPDATE)), lit(3))
                                .when(lower(col(FDI_CHANGE_TYPE)).equalTo(lit(FDI_CHANGE_TYPE_INSERT)), lit(2))
                                .when(lower(col(FDI_CHANGE_TYPE)).equalTo(lit(FDI_CHANGE_TYPE_DELETE)), lit(1))
                                .otherwise(lit(0))
                );

        return withPriority
                .withColumn(rowNumberColumn, row_number().over(w))
                .filter(col(rowNumberColumn).equalTo(lit(1)))
                .drop(rowNumberColumn, changePriorityColumn);
    }
}
