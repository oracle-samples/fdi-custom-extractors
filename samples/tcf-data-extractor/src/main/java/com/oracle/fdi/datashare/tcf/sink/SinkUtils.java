package com.oracle.fdi.datashare.tcf.sink;

import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

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
     * Deduplicate incoming change rows by primary key with preference order:
     * update (3) > insert (2) > delete (1). Assumes single-SCN batches,
     * so SCN is not used as a tie-breaker.
     */
    public static Dataset<Row> dedupeChangesets(Dataset<Row> changesets, DatasetSchema schema) {
        List<String> pkCols = schema.getPrimaryKeyList();
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
