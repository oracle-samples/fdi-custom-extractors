package com.oracle.fdi.spark.sql.fditcf.source;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
import java.util.Set;

import com.oracle.fdi.spark.sql.fditcf.source.FdiTcfOptions;
import com.oracle.fdi.spark.sql.fditcf.source.FdiTcfScanBuilder;

public class FdiTcfTable implements Table, SupportsRead {

    private final Table parquetTable;
    private final FdiTcfOptions fditcfOptions; // retained options from DataSource

    public FdiTcfTable(Table parquetTable, FdiTcfOptions options) {
        this.parquetTable = parquetTable;
        this.fditcfOptions = options;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        if (parquetTable instanceof SupportsRead) {
            ScanBuilder delegateScanBuilder = ((SupportsRead) parquetTable).newScanBuilder(options);
            // Prefer retained, pre-parsed options if available to avoid reparsing
            if (this.fditcfOptions != null) {
                return new FdiTcfScanBuilder(delegateScanBuilder, this.fditcfOptions);
            }
            return new FdiTcfScanBuilder(delegateScanBuilder, options);
        }
        throw new UnsupportedOperationException("Underlying table does not support read");
    }

    @Override
    public String name() {
        return "fditcf";
    }

    @Override
    public StructType schema() {
        return parquetTable.schema();
    }

    @Override
    public Transform[] partitioning() {
        return parquetTable.partitioning();
    }

    @Override
    public Map<String, String> properties() {
        return parquetTable.properties();
    }

    @Override
    public Set<TableCapability> capabilities() {
        return parquetTable.capabilities();
    }
}
