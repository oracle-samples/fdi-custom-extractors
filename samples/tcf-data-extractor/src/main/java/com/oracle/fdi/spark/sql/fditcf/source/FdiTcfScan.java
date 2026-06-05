package com.oracle.fdi.spark.sql.fditcf.source;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.sql.execution.datasources.v2.FileScan;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oracle.fdi.spark.sql.fditcf.source.FdiTcfOptions;

public class FdiTcfScan implements Scan, Batch {

    private static final Logger log = LoggerFactory.getLogger(FdiTcfScan.class);

    private final Scan delegate;
    private final FdiTcfOptions fditcfOptions;

    public FdiTcfScan(Scan parquetScan){
        this(parquetScan, null);
    }

    public FdiTcfScan(Scan parquetScan, FdiTcfOptions options){
        this.delegate = parquetScan;
        this.fditcfOptions = options;
    }

    @Override
    public StructType readSchema(){
        return delegate.readSchema();
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        // Delegate to underlying scan's Batch implementation (e.g., ParquetScan)
        InputPartition[] original = delegate.toBatch().planInputPartitions();

        // If no options or no range specified, just return delegate's partitions
        if (fditcfOptions == null || (!fditcfOptions.hasStartingScn() && !fditcfOptions.hasEndingScn())) {
            return original;
        }

        final Long start = fditcfOptions.getStartingScn();
        final Long end = fditcfOptions.getEndingScn();
        log.info("Applying fdi_scn_id filter on partition values: start={}, end={}", start, end);

        List<InputPartition> filtered = new ArrayList<>();
        int inFiles = 0;
        int outFiles = 0;

        for (InputPartition ip : original) {
            FilePartition fp = (FilePartition) ip;
            List<PartitionedFile> kept = new ArrayList<>();

            for (PartitionedFile pf : fp.files()) {
                inFiles++;
                Map<String, String> parts = extractPartitionMapFromPath(pf.filePath().toString());

                // Prefer SCN from parsed partition map (only fdi_scn_id)
                Long scn = null;
                String scnStr = parts.get("fdi_scn_id");
                if (scnStr != null) {
                    try { scn = Long.parseLong(scnStr); } catch (NumberFormatException ignore) { scn = null; }
                }

                boolean keep = scn != null;
                if (keep && start != null && scn < start) keep = false;
                if (keep && end != null && scn > end) keep = false;

                if (keep) {
                    kept.add(pf);
                    outFiles++;
                }
            }

            if (!kept.isEmpty()) {
                PartitionedFile[] arr = kept.toArray(new PartitionedFile[0]);
                FilePartition newFp = FilePartition.apply(fp.index(), arr);
                filtered.add(newFp);
            }
        }

        log.info("Filtered files by fdi_scn_id: in={}, kept={}", inFiles, outFiles);
        InputPartition[] result = filtered.toArray(new InputPartition[0]);
        return result;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        // Delegate to underlying scan's Batch implementation (e.g., ParquetScan)
        return delegate.toBatch().createReaderFactory();
    }

    // Given a file path, extract all partition key=value segments and return as a Map
    // Example: /base/fdi_scn_id=1758901227176/fdi_table_change_type=transient_refresh/fdi_change_type=insert/part-00000.parquet
    // Returns: {fdi_scn_id=1758901227176, fdi_table_change_type=transient_refresh, fdi_change_type=insert}
    private Map<String, String> extractPartitionMapFromPath(String filePath) {
        LinkedHashMap<String, String> out = new LinkedHashMap<>();
        if (filePath == null || filePath.isEmpty()) return out;

        Path p = new Path(filePath);
        List<String> segments = new LinkedList<>();
        for (Path cur = p; cur != null; cur = cur.getParent()) {
            String name = cur.getName();
            if (name != null && !name.isEmpty()) {
                // insert at front to keep left-to-right order
                segments.add(0, name);
            }
        }

        for (String seg : segments) {
            int eq = seg.indexOf('=');
            if (eq > 0 && eq < seg.length() - 1) {
                String key = seg.substring(0, eq);
                String val = seg.substring(eq + 1);
                if (!key.isEmpty() && !val.isEmpty()) {
                    out.put(key, val);
                }
            }
        }
        return out;
    }
}
