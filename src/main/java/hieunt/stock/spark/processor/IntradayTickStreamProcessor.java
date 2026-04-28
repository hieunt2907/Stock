package hieunt.stock.spark.processor;

import hieunt.stock.config.KafKaConfig;
import hieunt.stock.spark.Spark;
import hieunt.stock.spark.sink.ClickhouseSink;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class IntradayTickStreamProcessor implements BaseProcessor {

    private static final String TOPIC   = "intraday_tick";
    private static final String TABLE   = "stock.fact_intraday_tick";
    private static final String TRIGGER = "15 seconds";
    private static final String CKPT    = "/tmp/checkpoints/intraday_tick";

    // All fields arrive as strings from JSON; we cast them after parsing.
    private static final StructType SCHEMA = DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("ticker",      DataTypes.StringType, false),
        DataTypes.createStructField("time",        DataTypes.StringType, true),
        DataTypes.createStructField("price",       DataTypes.StringType, true),
        DataTypes.createStructField("volume",      DataTypes.StringType, true),
        DataTypes.createStructField("match_type",  DataTypes.StringType, true),
        DataTypes.createStructField("buy_vol",     DataTypes.StringType, true),
        DataTypes.createStructField("sell_vol",    DataTypes.StringType, true),
        DataTypes.createStructField("ingested_at", DataTypes.StringType, true),
    });

    private final ClickhouseSink sink;

    public IntradayTickStreamProcessor() {
        this.sink = new ClickhouseSink();
    }

    @Override
    public void process() {
        SparkSession spark = Spark.getSparkSession();

        Dataset<Row> raw = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", KafKaConfig.ADDRESS)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load();

        Dataset<Row> transformed = raw
            .selectExpr("CAST(value AS STRING) AS json_str")
            .select(functions.from_json(functions.col("json_str"), SCHEMA).as("d"))
            .select("d.*")
            // Drop records without the two primary key fields
            .filter(functions.col("ticker").isNotNull().and(functions.col("time").isNotNull()))
            // Cast to target types; nulls from failed casts are handled with coalesce below
            .withColumn("time",        functions.to_timestamp(functions.col("time")))
            .withColumn("price",       functions.col("price").cast(DataTypes.createDecimalType(20, 2)))
            .withColumn("volume",      functions.coalesce(
                functions.col("volume").cast(DataTypes.LongType), functions.lit(0L)))
            .withColumn("buy_vol",     functions.coalesce(
                functions.col("buy_vol").cast(DataTypes.LongType), functions.lit(0L)))
            .withColumn("sell_vol",    functions.coalesce(
                functions.col("sell_vol").cast(DataTypes.LongType), functions.lit(0L)))
            .withColumn("match_type",  functions.coalesce(
                functions.col("match_type"), functions.lit(" ")))
            .withColumn("ingested_at", functions.to_timestamp(functions.col("ingested_at")))
            // Drop rows where time could not be parsed
            .filter(functions.col("time").isNotNull());

        try {
            transformed.writeStream()
                .trigger(Trigger.ProcessingTime(TRIGGER))
                .option("checkpointLocation", CKPT)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDf, batchId) -> {
                    if (batchDf.isEmpty()) return;
                    Dataset<Row> deduped = batchDf.dropDuplicates("ticker", "time");
                    System.out.printf("[Batch %d] Writing %d tick rows → %s%n",
                        batchId, deduped.count(), TABLE);
                    sink.write(deduped, TABLE, SaveMode.Append);
                })
                .start()
                .awaitTermination();
        } catch (Exception e) {
            throw new RuntimeException("Streaming query terminated unexpectedly", e);
        }
    }
}
