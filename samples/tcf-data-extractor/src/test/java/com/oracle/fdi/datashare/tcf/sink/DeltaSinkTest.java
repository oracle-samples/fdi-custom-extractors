package com.oracle.fdi.datashare.tcf.sink;
import static org.junit.Assert.assertEquals;
import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;
import io.delta.tables.DeltaTable;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
public class DeltaSinkTest {
    private static SparkSession spark;
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @BeforeClass
    public static void setUpSpark() {
        spark = SparkSession.builder()
                .appName("DeltaSinkTest")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
    }
    @AfterClass
    public static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }
    @Test
    public void deletesByDateEffectiveColumnsWhenPrimaryKeyIsNotPublished() throws Exception {
        String basePath = temporaryFolder.newFolder("date-effective").getAbsolutePath();
        DeltaSink sink = sink(basePath);
        DatasetSchema schema = schema("ID", "START_DATE, END_DATE");
        sink.applyChangeSets("ORDERS", spark.sql("""
                SELECT 1 AS ID, DATE '2026-01-01' AS START_DATE, DATE '2026-12-31' AS END_DATE,
                       'first' AS NAME, 'insert' AS fdi_change_type,
                       'full' AS fdi_table_change_type, 1L AS fdi_scn_id
                UNION ALL
                SELECT 2, DATE '2026-01-01', DATE '2026-12-31', 'second', 'insert', 'full', 1L
                UNION ALL
                SELECT 3, DATE '2027-01-01', DATE '2027-12-31', 'third', 'insert', 'full', 1L
                """), schema);
        sink.applyChangeSets("ORDERS", spark.sql("""
                SELECT DATE '2026-01-01' AS START_DATE, DATE '2026-12-31' AS END_DATE,
                       'delete' AS fdi_change_type, 'incremental' AS fdi_table_change_type,
                       2L AS fdi_scn_id
                """), schema);
        List<Row> remaining = readTable(basePath, "ORDERS").orderBy("ID").collectAsList();
        assertEquals(1, remaining.size());
        assertEquals(3, remaining.get(0).getInt(0));
    }
    @Test
    public void preservesPrimaryKeyDeletesForExistingSchemas() throws Exception {
        String basePath = temporaryFolder.newFolder("primary-key").getAbsolutePath();
        DeltaSink sink = sink(basePath);
        DatasetSchema schema = schema("ID", null);
        sink.applyChangeSets("ORDERS", spark.sql("""
                SELECT 1 AS ID, DATE '2026-01-01' AS START_DATE, DATE '2026-12-31' AS END_DATE,
                       'first' AS NAME, 'insert' AS fdi_change_type,
                       'full' AS fdi_table_change_type, 1L AS fdi_scn_id
                UNION ALL
                SELECT 2, DATE '2026-01-01', DATE '2026-12-31', 'second', 'insert', 'full', 1L
                """), schema);
        sink.applyChangeSets("ORDERS", spark.sql("""
                SELECT 1 AS ID, 'delete' AS fdi_change_type,
                       'incremental' AS fdi_table_change_type, 2L AS fdi_scn_id
                """), schema);
        List<Row> remaining = readTable(basePath, "ORDERS").orderBy("ID").collectAsList();
        assertEquals(1, remaining.size());
        assertEquals(2, remaining.get(0).getInt(0));
    }
    @Test
    public void mergesTreeCodeWhenPublishedColumnsHaveNoCxPrefix() throws Exception {
        String basePath = temporaryFolder.newFolder("tree-code").getAbsolutePath();
        DeltaSink sink = sink(basePath);
        DatasetSchema schema = treeSchema();

        sink.applyChangeSets("TREES", spark.sql("""
                SELECT 'sample' AS createdby, 'record-1' AS source_record_id,
                       'STRUCTURE' AS treestructurecode, 'description' AS description,
                       'US' AS language, 'login' AS lastupdatelogin,
                       'US' AS sourcelang, 'sample' AS lastupdatedby,
                       'TREE_A' AS treecode, 'first' AS treename,
                       TIMESTAMP '2026-01-01 00:00:00' AS creationdate,
                       TIMESTAMP '2026-01-01 00:00:00' AS lastupdatedate,
                       'insert' AS fdi_change_type, 'full' AS fdi_table_change_type,
                       1788911667000L AS fdi_scn_id
                """), schema);

        List<Row> rows = readTable(basePath, "TREES").select("treecode", "treename").collectAsList();
        assertEquals(1, rows.size());
        assertEquals("TREE_A", rows.get(0).getString(0));
        assertEquals("first", rows.get(0).getString(1));
        assertEquals("CX_TREECODE", schema.getPrimaryKeyCols());
        assertEquals("CX_TreeCode", schema.getSchema().get(8).getColName());
    }

    @Test
    public void deduplicatesIncrementalRowsUsingPublishedTreeCode() throws Exception {
        String basePath = temporaryFolder.newFolder("tree-incremental").getAbsolutePath();
        DeltaSink sink = sink(basePath);
        DatasetSchema schema = treeSchema();

        sink.applyChangeSets("TREES", spark.sql("""
                SELECT 'TREE_A' AS treecode, 'first' AS treename,
                       'insert' AS fdi_change_type, 'full' AS fdi_table_change_type,
                       1L AS fdi_scn_id
                """), schema);
        sink.applyChangeSets("TREES", spark.sql("""
                SELECT 'TREE_A' AS treecode, 'ignored' AS treename,
                       'insert' AS fdi_change_type, 'incremental' AS fdi_table_change_type,
                       2L AS fdi_scn_id
                UNION ALL
                SELECT 'TREE_A', 'updated', 'update', 'incremental', 2L
                """), schema);

        List<Row> rows = readTable(basePath, "TREES").select("treecode", "treename").collectAsList();
        assertEquals(1, rows.size());
        assertEquals("TREE_A", rows.get(0).getString(0));
        assertEquals("updated", rows.get(0).getString(1));
    }

    @Test
    public void deletesUsingPublishedTreeCode() throws Exception {
        String basePath = temporaryFolder.newFolder("tree-delete").getAbsolutePath();
        DeltaSink sink = sink(basePath);
        DatasetSchema schema = treeSchema();

        sink.applyChangeSets("TREES", spark.sql("""
                SELECT 'TREE_A' AS treecode, 'first' AS treename,
                       'insert' AS fdi_change_type, 'full' AS fdi_table_change_type,
                       1L AS fdi_scn_id
                UNION ALL
                SELECT 'TREE_B', 'second', 'insert', 'full', 1L
                """), schema);
        sink.applyChangeSets("TREES", spark.sql("""
                SELECT 'TREE_A' AS treecode, 'delete' AS fdi_change_type,
                       'incremental' AS fdi_table_change_type, 2L AS fdi_scn_id
                """), schema);

        List<Row> rows = readTable(basePath, "TREES").select("treecode").collectAsList();
        assertEquals(1, rows.size());
        assertEquals("TREE_B", rows.get(0).getString(0));
    }

    @Test
    public void deletesUsingPublishedDateEffectiveColumns() throws Exception {
        String basePath = temporaryFolder.newFolder("prefixed-date-effective").getAbsolutePath();
        DeltaSink sink = sink(basePath);
        DatasetSchema schema = schema("CX_ID", "CX_START_DATE, CX_END_DATE");
        schema.getSchema().forEach(column -> column.setColName("CX_" + column.getColName()));

        sink.applyChangeSets("ORDERS", spark.sql("""
                SELECT 1 AS id, DATE '2026-01-01' AS start_date, DATE '2026-12-31' AS end_date,
                       'first' AS name, 'insert' AS fdi_change_type,
                       'full' AS fdi_table_change_type, 1L AS fdi_scn_id
                UNION ALL
                SELECT 2, DATE '2026-01-01', DATE '2026-12-31', 'second', 'insert', 'full', 1L
                UNION ALL
                SELECT 3, DATE '2027-01-01', DATE '2027-12-31', 'third', 'insert', 'full', 1L
                """), schema);
        sink.applyChangeSets("ORDERS", spark.sql("""
                SELECT DATE '2026-01-01' AS start_date, DATE '2026-12-31' AS end_date,
                       'delete' AS fdi_change_type, 'incremental' AS fdi_table_change_type,
                       2L AS fdi_scn_id
                """), schema);

        List<Row> remaining = readTable(basePath, "ORDERS").orderBy("id").collectAsList();
        assertEquals(1, remaining.size());
        assertEquals(3, remaining.get(0).getInt(0));
    }

    @Test
    public void preservesLiteralCxKeyWhenBothColumnNamesExist() throws Exception {
        String basePath = temporaryFolder.newFolder("literal-cx-key").getAbsolutePath();
        DeltaSink sink = sink(basePath);
        DatasetSchema schema = schema("CX_TREECODE", null);
        schema.setSchema(List.of(column("CX_TREECODE"), column("treecode"), column("name")));

        sink.applyChangeSets("TREES", spark.sql("""
                SELECT 'KEY_A' AS CX_TREECODE, 'non-key' AS treecode, 'first' AS name,
                       'insert' AS fdi_change_type, 'full' AS fdi_table_change_type,
                       1L AS fdi_scn_id
                UNION ALL
                SELECT 'KEY_B', 'non-key', 'second', 'insert', 'full', 1L
                """), schema);
        sink.applyChangeSets("TREES", spark.sql("""
                SELECT 'KEY_A' AS CX_TREECODE, 'changed-non-key' AS treecode, 'updated' AS name,
                       'update' AS fdi_change_type, 'incremental' AS fdi_table_change_type,
                       2L AS fdi_scn_id
                """), schema);

        List<Row> remaining = readTable(basePath, "TREES").orderBy("CX_TREECODE").collectAsList();
        assertEquals(2, remaining.size());
        assertEquals("KEY_A", remaining.get(0).getString(0));
        assertEquals("updated", remaining.get(0).getString(2));
        assertEquals("second", remaining.get(1).getString(2));
    }

    private DatasetSchema treeSchema() {
        DatasetSchema schema = new DatasetSchema();
        schema.setTableName("DW_FA_X_FNDTREETLPVO");
        schema.setPrimaryKeyCols("CX_TREECODE");
        schema.setFullRefresh(true);
        schema.setSchema(List.of(
                column("CX_CreatedBy"), column("CX_CreationDate"), column("CX_Description"),
                column("CX_Language"), column("CX_LastUpdateDate"), column("CX_LastUpdateLogin"),
                column("CX_LastUpdatedBy"), column("CX_SourceLang"), column("CX_TreeCode"),
                column("CX_TreeName"), column("CX_TreeStructureCode"), column("SOURCE_RECORD_ID")));
        return schema;
    }

    private DeltaSink sink(String basePath) {
        PipelineManifest.SinkConfig config = new PipelineManifest.SinkConfig();
        config.setType("LOCAL_FILESYSTEM");
        config.setFormat("delta_lake");
        config.setConfig(Map.of("path", basePath));
        DeltaSink sink = new DeltaSink(config);
        sink.initialize(spark);
        return sink;
    }
    private Dataset<Row> readTable(String basePath, String datasetName) {
        return DeltaTable.forPath(spark, basePath + "/" + datasetName).toDF();
    }
    private DatasetSchema schema(String primaryKeyCols, String dateEffectiveCols) {
        DatasetSchema schema = new DatasetSchema();
        schema.setPrimaryKeyCols(primaryKeyCols);
        schema.setDateEffectiveCols(dateEffectiveCols);
        schema.setSchema(List.of(
                column("ID"),
                column("START_DATE"),
                column("END_DATE"),
                column("NAME")));
        return schema;
    }
    private DatasetSchema.Column column(String name) {
        DatasetSchema.Column column = new DatasetSchema.Column();
        column.setColName(name);
        return column;
    }
}
