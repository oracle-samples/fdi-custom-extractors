package com.oracle.fdi.spark.sql.fditcf.source;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oracle.fdi.spark.sql.fditcf.source.FdiTcfOptions;

/**
 * DataSourceRegister: Provides the "short name" (alias) for .format()
 * TableProvider: Provides the logic to return a Table/DataFrame
 */
public class FdiTcfDataSource implements TableProvider, DataSourceRegister {

    private static Logger log = LoggerFactory.getLogger(FdiTcfDataSource.class);

    // Marker constants
    private static final String FULL_REFRESH_MARKER_PREFIX = "_latest_full_refresh_scn_";
    private static final String LATEST_SCN_MARKER_PREFIX   = "_latest_scn_";
    private static final String MARKER_SUFFIX = ".marker";

    // Internal delegation to native Parquet
    private final ParquetDataSourceV2 delegate = new ParquetDataSourceV2();

    @Override
    public String shortName() {
        return "fdi-tcf";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        // Reuse the same path restriction used at table construction time
        Map<String, String> mutable = new HashMap<>(options.asCaseSensitiveMap());
        Map<String, String> updated = applyPathRestriction(mutable);
        return delegate.inferSchema(new CaseInsensitiveStringMap(updated));
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        // Reuse the same path restriction so only selected directories are indexed
        Map<String, String> updated = applyPathRestriction(new HashMap<>(properties));
        FdiTcfOptions fdiIdfOptions = FdiTcfOptions.parse(updated);
        Table parquetTable = delegate.getTable(schema, partitioning, updated);

        return new FdiTcfTable(parquetTable, fdiIdfOptions);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    private Map<String, String> applyPathRestriction(Map<String, String> in) {
        Map<String, String> out = new HashMap<>(in);
        String base = out.get("path");
        if (base != null) {
            // Derive from marker files under basePath/markers as needed
            SparkSession spark = SparkSession.active();
            Path basePath = new Path(base);

            // Resolve startingVersion strictly from latest full refresh marker
            Optional<Long> latestFull = findLatestFullRefreshScn(basePath, spark);
            Optional<Long> latestVersion = findLatestScn(basePath, spark);

            if (!latestFull.isPresent() || !latestVersion.isPresent()) {
                throw new RuntimeException("Markers are corrupted");
            }

            String selected = getSelectedPaths(basePath, latestFull.get(), latestVersion.get());
            applySelectedPaths(out, base, selected);
        }
        return out;
    }

    private String getSelectedPaths(Path basePath, Long start, Long end) {
        SparkSession spark = SparkSession.active();

        List<String> selectedPaths = listFiles(basePath, spark, folderName -> {
            Long scn = extractScn(folderName);
            if (scn == null)
                return false;
            if (start != null && scn < start)
                return false;
            if (end != null && scn > end)
                return false;
            return true;
        });
        return String.join(",", selectedPaths);
    }

    private static Long extractScn(String folderName) {
        if (folderName == null) return null;
        // Only support legacy-style partition naming "fdi_scn_id=<value>"
        String prefix = "fdi_scn_id=";
        int idx = folderName.indexOf(prefix);
        if (idx >= 0) {
            String num = folderName.substring(idx + prefix.length());
            try {
                return Long.parseLong(num);
            } catch (NumberFormatException ignore) {
                return null;
            }
        }
        return null;
    }

    private static List<String> listFiles(Path basePath, SparkSession spark, Predicate<String> filterCondition) {
        try {
            FileSystem fs = basePath.getFileSystem(spark.sparkContext().hadoopConfiguration());

            FileStatus[] statuses = fs.listStatus(basePath);
            return Arrays.stream(statuses)
                    .filter(status -> status.isDirectory())
                    .filter(status -> filterCondition.test(status.getPath().getName()))
                    .map(status -> status.getPath().toString())
                    .sorted()
                    .collect(Collectors.toList());

        } catch (Exception ex) {
            throw new RuntimeException("Failed to list directories under " + basePath, ex);
        }
    }

    // Helper: set single or multiple input paths correctly into options
    private void applySelectedPaths(Map<String, String> out, String base, String selectedPaths) {
        try {

            String selected = StringUtils.stripToEmpty(selectedPaths);
            if (StringUtils.isNotEmpty(selected)) {
                out.put("basePath", base);
                String[] arr = selected.split(",");
                String json = new ObjectMapper().writeValueAsString(arr);
                out.remove("path");
                out.put("paths", json);
                log.info("applyPathRestriction: basePath={}, paths(json)={}", base, json);
            } else {
                log.info("applyPathRestriction: No selected paths");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to apply selected paths", ex);
        }
    }

    // List file names (not directories) under a path
    private static List<String> listFileNames(Path path, SparkSession spark) {
        try {
            FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
            FileStatus[] statuses = fs.listStatus(path);
            return Arrays.stream(statuses)
                    .filter(status -> status.isFile())
                    .map(status -> status.getPath().getName())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private static String extractMarkerScn(String filename, String prefix) {
        return filename.substring(prefix.length(), filename.length() - MARKER_SUFFIX.length());
    }

    private java.util.Optional<Long> findLatestFullRefreshScn(Path basePath, SparkSession spark) {
        try {
            Path markersPath = new Path(basePath, "markers");
            List<String> names = listFileNames(markersPath, spark);
            return names.stream()
                    .filter(n -> n.startsWith(FULL_REFRESH_MARKER_PREFIX) && n.endsWith(MARKER_SUFFIX))
                    .map(n -> extractMarkerScn(n, FULL_REFRESH_MARKER_PREFIX))
                    .filter(s -> s != null && !s.isEmpty())
                    .map(Long::parseLong)
                    .max(Long::compareTo);
        } catch (Exception e) {
            return java.util.Optional.empty();
        }
    }

    private java.util.Optional<Long> findLatestScn(Path basePath, SparkSession spark) {
        try {
            Path markersPath = new Path(basePath, "markers");
            List<String> names = listFileNames(markersPath, spark);
            return names.stream()
                    .filter(n -> n.startsWith(LATEST_SCN_MARKER_PREFIX) && n.endsWith(MARKER_SUFFIX))
                    .map(n -> extractMarkerScn(n, LATEST_SCN_MARKER_PREFIX))
                    .filter(s -> s != null && !s.isEmpty())
                    .map(Long::parseLong)
                    .max(Long::compareTo);
        } catch (Exception e) {
            return java.util.Optional.empty();
        }
    }
}
