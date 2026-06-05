package com.oracle.fdi.datashare.tcf.common;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String toJson(Object obj) {
        String json = "";
        try {
            json = mapper.writeValueAsString(obj);
        } catch (Exception ex) {
            log.error("Failed to serialize object to JSON", ex);
        }
        return json;
    }

    public static List<String> listFiles(Path basePath, SparkSession spark, boolean onlyDirs) {
        try {
            FileSystem fs = basePath.getFileSystem(spark.sparkContext().hadoopConfiguration());
            FileStatus[] statuses = fs.listStatus(basePath);
            return Arrays.stream(statuses)
                    .filter(status -> !onlyDirs || status.isDirectory())
                    .map(status -> status.getPath().getName())
                    .sorted()
                    .collect(Collectors.toList());
        } catch (Exception ex) {
            throw new RuntimeException("Exception in listFiles " + basePath, ex);
        }
    }

    public static void createDirIfNotExist(SparkSession spark, Path path){
        try {
            FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
        } catch (Exception ex) {
            throw new RuntimeException("No permission to create required directory", ex);
        }
    }

    public static boolean exists(SparkSession spark, Path path) {
        try {
            FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
            return fs.exists(path);
        } catch (Exception ex) {
            throw new RuntimeException("Exception in exists " + path, ex);
        }
    }

    public static void writeObjAsJson(SparkSession spark, Path path, Object obj, boolean overwrite) {
        try {
            FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
            Path parent = path.getParent();
            if (parent != null && !fs.exists(parent)) {
                fs.mkdirs(parent);
            }
            try (FSDataOutputStream os = fs.create(path, overwrite)) {
                mapper.writeValue((OutputStream) os, obj);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Exception in writeObjAsJson " + path, ex);
        }
    }

    public static <T> T jsonToObj(SparkSession spark, Path path, Class<T> objectClass) {
        try {
            FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
            try (InputStream is = fs.open(path)) {
                return mapper.readValue(is, objectClass);
            }
        } catch (Exception ex) {
            log.error("Failed to read JSON from path {}", path, ex);
            return null;
        }
    }

    public static <T> T requireNonNull(T value, String field) {
        if (value == null) {
            throw new IllegalArgumentException("Invalid pipeline manifest: '" + field + "' must be provided");
        }
        return value;
    }

    public static String requireNonBlank(String value, String field) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException("Invalid pipeline manifest: '" + field + "' must be provided");
        }
        return value;
    }
}
