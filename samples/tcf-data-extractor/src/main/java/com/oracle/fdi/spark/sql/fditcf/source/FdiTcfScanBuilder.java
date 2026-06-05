package com.oracle.fdi.spark.sql.fditcf.source;

import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class FdiTcfScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns {

  private final ScanBuilder delegate;
  private final FdiTcfOptions fdiIdfOptions;

  public FdiTcfScanBuilder(ScanBuilder delegate, CaseInsensitiveStringMap options) {
    this.delegate = delegate;
    this.fdiIdfOptions = FdiTcfOptions.parse(options);
  }

  // Overload to avoid reparsing when options already parsed upstream
  public FdiTcfScanBuilder(ScanBuilder delegate, FdiTcfOptions options) {
    this.delegate = delegate;
    this.fdiIdfOptions = options;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    if (delegate instanceof SupportsPushDownRequiredColumns) {
      ((SupportsPushDownRequiredColumns) delegate).pruneColumns(requiredSchema);
    }
  }

  @Override
  public Scan build() {
    // use the correct field name here
    return new FdiTcfScan(delegate.build(), fdiIdfOptions);
  }
}
