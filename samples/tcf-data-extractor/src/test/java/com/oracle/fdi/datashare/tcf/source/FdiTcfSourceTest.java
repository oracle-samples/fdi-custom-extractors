package com.oracle.fdi.datashare.tcf.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.oracle.fdi.datashare.tcf.common.TcfPipelineException;
import com.oracle.fdi.datashare.tcf.common.Utils;
import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;
import com.oracle.fdi.datashare.tcf.pipeline.TcfPipeline;
import com.oracle.fdi.datashare.tcf.watermark.JsonBasedWaterMark;
import io.delta.tables.DeltaTable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FdiTcfSourceTest {

    private static SparkSession spark;
    private static final long FIRST_SCN = 1788911667000L;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Path sourceRoot;
    private Path deltaRoot;
    private Path watermarkRoot;

    @BeforeClass
    public static void setUpSpark() {
        spark = SparkSession.builder()
                .appName("FdiTcfSourceTest")
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

    @Before
    public void setUpPaths() throws Exception {
        sourceRoot = temporaryFolder.newFolder("source").toPath();
        deltaRoot = temporaryFolder.newFolder("delta").toPath();
        watermarkRoot = temporaryFolder.newFolder("watermark").toPath();
    }

    @Test
    public void discoversCompletedScnWithoutParquetFiles() throws Exception {
        publish("ORDERS", FIRST_SCN, true, true);
        markers("ORDERS", FIRST_SCN, FIRST_SCN);

        assertEquals(List.of(FIRST_SCN), source().listPendingScnIdsAfter("ORDERS", 0));
        assertTrue(source().readChangeSets("ORDERS", FIRST_SCN).isEmpty());
    }

    @Test
    public void doesNotReadAlreadyProcessedEmptyScns() throws Exception {
        publish("ORDERS", FIRST_SCN, true, true);
        markers("ORDERS", FIRST_SCN, FIRST_SCN);

        assertEquals(List.of(), source().listPendingScnIdsAfter("ORDERS", FIRST_SCN));
    }

    @Test
    public void ignoresDirectoriesWithoutScns() throws Exception {
        Files.createDirectories(sourceRoot.resolve("STALE"));

        assertEquals(List.of(), source().listPendingScnIdsAfter("STALE", 0));
    }

    @Test
    public void ignoresAnInitialPublishThatHasNotCompleted() throws Exception {
        publish("ORDERS", FIRST_SCN, true, false);

        assertEquals(List.of(), source().listPendingScnIdsAfter("ORDERS", 0));
    }

    @Test
    public void stopsBeforeAnIncompleteScn() throws Exception {
        publish("ORDERS", FIRST_SCN, true, true);
        publish("ORDERS", FIRST_SCN + 1, false, false);
        publish("ORDERS", FIRST_SCN + 2, false, true);
        markers("ORDERS", FIRST_SCN, FIRST_SCN + 2);

        assertEquals(List.of(FIRST_SCN), source().listPendingScnIdsAfter("ORDERS", 0));
        assertThrows(IllegalStateException.class,
                () -> source().readChangeSets("ORDERS", FIRST_SCN + 1));
    }

    @Test
    public void honorsFullRefreshAndLatestScnMarkerBounds() throws Exception {
        publish("ORDERS", FIRST_SCN - 1, true, true);
        publish("ORDERS", FIRST_SCN, true, true);
        publish("ORDERS", FIRST_SCN + 1, false, true);
        publish("ORDERS", FIRST_SCN + 2, false, true);
        Files.createDirectories(sourceRoot.resolve("ORDERS/fdi_scn_id=invalid"));
        Files.createDirectories(sourceRoot.resolve("ORDERS/archived_fdi_scn_id=" + FIRST_SCN));
        markers("ORDERS", FIRST_SCN, FIRST_SCN + 1);

        assertEquals(List.of(FIRST_SCN, FIRST_SCN + 1), source().listPendingScnIdsAfter("ORDERS", 0));
    }

    @Test
    public void ignoresTemporaryParquetFilesInAnEmptyPublish() throws Exception {
        Path scn = publish("ORDERS", FIRST_SCN, true, true);
        Path temporary = Files.createDirectories(scn.resolve("_temporary"));
        Files.writeString(temporary.resolve("part.parquet"), "uncommitted");
        markers("ORDERS", FIRST_SCN, FIRST_SCN);

        assertTrue(source().readChangeSets("ORDERS", FIRST_SCN).isEmpty());
    }

    @Test
    public void readsParquetFilesWithoutAParquetSuffix() throws Exception {
        Path scn = publish("ORDERS", FIRST_SCN, true, true);
        writeData(scn, true, false);
        Path data = scn.resolve("fdi_table_change_type=full/fdi_change_type=insert");
        try (java.util.stream.Stream<Path> files = Files.list(data)) {
            Path parquet = files.filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .findFirst().orElseThrow();
            Files.move(parquet, data.resolve("part-00000"));
        }
        markers("ORDERS", FIRST_SCN, FIRST_SCN);

        assertEquals(1, source().readChangeSets("ORDERS", FIRST_SCN).count());
    }

    @Test
    public void emptyDatasetDoesNotBlockAnotherDataset() throws Exception {
        publish("A_EMPTY", FIRST_SCN, true, true);
        markers("A_EMPTY", FIRST_SCN, FIRST_SCN);
        Path data = publish("Z_ORDERS", FIRST_SCN, true, true);
        writeData(data, true, false);
        markers("Z_ORDERS", FIRST_SCN, FIRST_SCN);

        runPipeline();

        assertEquals(Optional.of(FIRST_SCN), watermark().getLastAppliedSCN("A_EMPTY"));
        assertEquals(Optional.of(FIRST_SCN), watermark().getLastAppliedSCN("Z_ORDERS"));
        assertFalse(DeltaTable.isDeltaTable(spark, deltaRoot.resolve("A_EMPTY").toString()));
        assertEquals(1, target("Z_ORDERS").count());
    }

    @Test
    public void createsTargetOnFirstDataAfterAnEmptyFullRefresh() throws Exception {
        publish("ORDERS", FIRST_SCN, true, true);
        Path incremental = publish("ORDERS", FIRST_SCN + 1, false, true);
        writeData(incremental, false, false);
        markers("ORDERS", FIRST_SCN, FIRST_SCN + 1);

        runPipeline();

        assertEquals(1, target("ORDERS").count());
        assertEquals(Optional.of(FIRST_SCN + 1), watermark().getLastAppliedSCN("ORDERS"));
    }

    @Test
    public void emptyFullRefreshClearsExistingTarget() throws Exception {
        seedTarget("ORDERS");
        publish("ORDERS", FIRST_SCN, true, true);
        markers("ORDERS", FIRST_SCN, FIRST_SCN);

        runPipeline();

        assertEquals(0, target("ORDERS").count());
        assertEquals(Optional.of(FIRST_SCN), watermark().getLastAppliedSCN("ORDERS"));
    }

    @Test
    public void zeroRowParquetFullRefreshClearsExistingTarget() throws Exception {
        seedTarget("ORDERS");
        Path scn = publish("ORDERS", FIRST_SCN, true, true);
        writeData(scn, true, true);
        markers("ORDERS", FIRST_SCN, FIRST_SCN);

        runPipeline();

        assertEquals(0, target("ORDERS").count());
        assertEquals(Optional.of(FIRST_SCN), watermark().getLastAppliedSCN("ORDERS"));
    }

    @Test
    public void zeroRowParquetCreatesAnEmptyTargetWhenSchemaIsAvailable() throws Exception {
        Path scn = publish("ORDERS", FIRST_SCN, true, true);
        writeData(scn, true, true);
        markers("ORDERS", FIRST_SCN, FIRST_SCN);

        runPipeline();

        assertEquals(0, target("ORDERS").count());
        assertEquals(Optional.of(FIRST_SCN), watermark().getLastAppliedSCN("ORDERS"));
    }

    @Test
    public void emptyIncrementalPreservesTargetAndAdvancesWatermark() throws Exception {
        seedTarget("ORDERS");
        publish("ORDERS", FIRST_SCN, true, true);
        publish("ORDERS", FIRST_SCN + 1, false, true);
        markers("ORDERS", FIRST_SCN, FIRST_SCN + 1);
        watermark().save("ORDERS", FIRST_SCN);

        runPipeline();

        assertEquals(1, target("ORDERS").count());
        assertEquals(Optional.of(FIRST_SCN + 1), watermark().getLastAppliedSCN("ORDERS"));
    }

    @Test
    public void missingSchemaDoesNotAdvanceWatermarkOrClearTarget() throws Exception {
        seedTarget("ORDERS");
        Path scn = Files.createDirectories(scnPath("ORDERS", FIRST_SCN));
        Files.createFile(scn.resolve("_DONE"));
        markers("ORDERS", FIRST_SCN, FIRST_SCN);

        assertThrows(TcfPipelineException.class, this::runPipeline);
        assertEquals(1, target("ORDERS").count());
        assertEquals(Optional.empty(), watermark().getLastAppliedSCN("ORDERS"));
    }

    @Test
    public void corruptParquetIsNotTreatedAsAnEmptyPublish() throws Exception {
        seedTarget("ORDERS");
        Path scn = publish("ORDERS", FIRST_SCN, true, true);
        Path data = Files.createDirectories(scn.resolve("fdi_table_change_type=full/fdi_change_type=insert"));
        Files.writeString(data.resolve("part.parquet"), "not parquet");
        markers("ORDERS", FIRST_SCN, FIRST_SCN);

        assertThrows(TcfPipelineException.class, this::runPipeline);
        assertEquals(1, target("ORDERS").count());
        assertEquals(Optional.empty(), watermark().getLastAppliedSCN("ORDERS"));
    }

    @Test
    public void emptyPublishWithoutRefreshMetadataFailsBeforeChangingTarget() throws Exception {
        seedTarget("ORDERS");
        publish("ORDERS", FIRST_SCN, null, true);
        markers("ORDERS", FIRST_SCN, FIRST_SCN);

        assertThrows(TcfPipelineException.class, this::runPipeline);
        assertEquals(1, target("ORDERS").count());
        assertEquals(Optional.empty(), watermark().getLastAppliedSCN("ORDERS"));
    }

    private Path publish(String dataset, long scn, Boolean fullRefresh, boolean complete) throws Exception {
        Path path = Files.createDirectories(scnPath(dataset, scn));
        DatasetSchema schema = new DatasetSchema();
        schema.setTableName(dataset);
        schema.setPrimaryKeyCols("ID");
        schema.setFullRefresh(fullRefresh);
        schema.setSchema(List.of(column("ID"), column("NAME")));
        Utils.writeObjAsJson(spark, hadoopPath(path.resolve("_schema.json")), schema, true);
        if (complete) {
            Files.createFile(path.resolve("_DONE"));
        }
        return path;
    }

    private DatasetSchema.Column column(String name) {
        DatasetSchema.Column column = new DatasetSchema.Column();
        column.setColName(name);
        return column;
    }

    private void markers(String dataset, long fullRefresh, long latest) throws Exception {
        Path path = Files.createDirectories(sourceRoot.resolve(dataset).resolve("markers"));
        Files.createFile(path.resolve("_latest_full_refresh_scn_" + fullRefresh + ".marker"));
        Files.createFile(path.resolve("_latest_scn_" + latest + ".marker"));
    }

    private void writeData(Path scn, boolean fullRefresh, boolean empty) {
        String changeType = fullRefresh ? "full" : "incremental";
        spark.sql("SELECT 1L AS ID, 'order' AS NAME" + (empty ? " WHERE false" : ""))
                .write().parquet(scn.resolve("fdi_table_change_type=" + changeType)
                        .resolve("fdi_change_type=insert").toString());
    }

    private Path scnPath(String dataset, long scn) {
        return sourceRoot.resolve(dataset).resolve("fdi_scn_id=" + scn);
    }

    private PipelineManifest.SourceConfig sourceConfig() {
        PipelineManifest.SourceConfig config = new PipelineManifest.SourceConfig();
        config.setType("LOCAL_FILESYSTEM");
        config.setFormat("fdi-tcf");
        config.setConfig(Map.of("path", sourceRoot.toString()));
        return config;
    }

    private FdiTcfSource source() {
        FdiTcfSource source = new FdiTcfSource(sourceConfig());
        source.initialize(spark);
        return source;
    }

    private JsonBasedWaterMark watermark() {
        return new JsonBasedWaterMark(spark, hadoopPath(watermarkRoot));
    }

    private void seedTarget(String dataset) {
        spark.sql("SELECT 1L AS ID, 'existing' AS NAME")
                .write().format("delta").save(deltaRoot.resolve(dataset).toString());
    }

    private Dataset<Row> target(String dataset) {
        return DeltaTable.forPath(spark, deltaRoot.resolve(dataset).toString()).toDF();
    }

    private void runPipeline() {
        PipelineManifest.SinkConfig sink = new PipelineManifest.SinkConfig();
        sink.setType("LOCAL_FILESYSTEM");
        sink.setFormat("delta_lake");
        sink.setConfig(Map.of("path", deltaRoot.toString()));
        PipelineManifest.WatermarkConfig watermark = new PipelineManifest.WatermarkConfig();
        watermark.setPath(watermarkRoot.toString());
        PipelineManifest manifest = new PipelineManifest();
        manifest.setName("empty-publish-test");
        manifest.setSource(sourceConfig());
        manifest.setSink(sink);
        manifest.setWatermark(watermark);
        org.apache.hadoop.fs.Path path = hadoopPath(temporaryFolder.getRoot().toPath().resolve("manifest.json"));
        Utils.writeObjAsJson(spark, path, manifest, true);
        TcfPipeline.build(path.toString(), spark).run();
    }

    private org.apache.hadoop.fs.Path hadoopPath(Path path) {
        return new org.apache.hadoop.fs.Path(path.toString());
    }
}
