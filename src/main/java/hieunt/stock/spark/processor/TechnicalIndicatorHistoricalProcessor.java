package hieunt.stock.spark.processor;

import hieunt.stock.config.ClickhouseConfig;
import hieunt.stock.spark.Spark;
import hieunt.stock.spark.sink.ClickhouseSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Properties;

/**
 * TechnicalIndicatorHistoricalProcessor
 *
 * Đọc toàn bộ dữ liệu từ stock.fact_daily_price (ClickHouse),
 * tính RSI(14) và SMA(200) theo từng ticker bằng Spark Window Functions,
 * sau đó ghi kết quả vào stock.technical_indicators_historical.
 *
 * RSI(14):
 *   - avg_gain = AVG của max(delta, 0) trong 14 phiên gần nhất
 *   - avg_loss = AVG của max(-delta, 0) trong 14 phiên gần nhất
 *   - RS = avg_gain / avg_loss
 *   - RSI = 100 - (100 / (1 + RS))
 *   - Null nếu chưa đủ 14 phiên.
 *
 * SMA(200):
 *   - AVG(close) trong 200 phiên gần nhất (rolling).
 *   - Null nếu chưa đủ 200 phiên.
 */
public class TechnicalIndicatorHistoricalProcessor implements BaseProcessor {

    private static final int RSI_PERIOD = 14;
    private static final int SMA_PERIOD = 200;

    private final ClickhouseSink clickhouseSink;

    public TechnicalIndicatorHistoricalProcessor() {
        this.clickhouseSink = new ClickhouseSink();
    }

    @Override
    public void process() {
        SparkSession spark = Spark.getSparkSession();

        System.out.println("TechnicalIndicatorHistoricalProcessor: reading fact_daily_price from ClickHouse...");

        // ── 1. Đọc toàn bộ fact_daily_price ──────────────────────────────────
        Properties props = new Properties();
        props.setProperty("user", ClickhouseConfig.USER);
        props.setProperty("password", ClickhouseConfig.PASSWORD);
        props.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver");

        Dataset<Row> raw = spark.read()
                .jdbc(ClickhouseConfig.getJdbcUrl(), "stock.fact_daily_price", props);

        if (raw == null || raw.isEmpty()) {
            System.out.println("No data in fact_daily_price. Aborting.");
            return;
        }

        // ── 2. Chuẩn hoá: chỉ giữ các cột cần thiết, đảm bảo kiểu dữ liệu ──
        Dataset<Row> priceDF = raw
                .select(
                        functions.col("ticker"),
                        functions.col("time").cast(DataTypes.TimestampType).as("time"),
                        functions.to_date(functions.col("time")).as("date_id"),
                        functions.col("close").cast(DataTypes.DoubleType).as("close")
                )
                .filter(functions.col("close").isNotNull())
                .filter(functions.col("close").gt(0));

        // ── 3. Window theo ticker, sắp xếp theo time tăng dần ────────────────
        WindowSpec tickerTimeWindow = Window
                .partitionBy("ticker")
                .orderBy("time");

        // ── 4. Tính delta close (close - close_prev) ──────────────────────────
        Dataset<Row> withDelta = priceDF
                .withColumn("close_prev",
                        functions.lag(functions.col("close"), 1).over(tickerTimeWindow))
                .withColumn("delta",
                        functions.col("close").minus(functions.col("close_prev")));

        // ── 5. Tách gain / loss ───────────────────────────────────────────────
        Dataset<Row> withGainLoss = withDelta
                .withColumn("gain",
                        functions.greatest(functions.col("delta"), functions.lit(0.0)))
                .withColumn("loss",
                        functions.greatest(functions.col("delta").multiply(-1), functions.lit(0.0)));

        // ── 6. Rolling window 14 phiên (row-based, không tính row hiện tại) ──
        //    rangeBetween / rowsBetween: -13 → 0 tức là 14 phiên kể cả hiện tại
        WindowSpec rsiWindow = Window
                .partitionBy("ticker")
                .orderBy("time")
                .rowsBetween(-(RSI_PERIOD - 1), 0);

        Dataset<Row> withRsiComponents = withGainLoss
                .withColumn("avg_gain", functions.avg(functions.col("gain")).over(rsiWindow))
                .withColumn("avg_loss", functions.avg(functions.col("loss")).over(rsiWindow));

        // ── 7. Tính RSI ────────────────────────────────────────────────────────
        //    Đặt null cho các phiên chưa đủ (14 phiên): dùng row_number() để kiểm tra
        WindowSpec rowNumWindow = Window
                .partitionBy("ticker")
                .orderBy("time");

        Dataset<Row> withRowNum = withRsiComponents
                .withColumn("row_num", functions.row_number().over(rowNumWindow));

        // RSI = 100 - 100 / (1 + RS), với RS = avg_gain / avg_loss
        // Nếu avg_loss == 0 và avg_gain > 0 → RSI = 100
        // Nếu avg_loss == 0 và avg_gain == 0 → RSI = 50 (neutral / không có biến động)
        Dataset<Row> withRsi = withRowNum.withColumn("rsi_14",
                functions.when(
                        functions.col("row_num").lt(RSI_PERIOD)
                                .or(functions.col("avg_gain").isNull())
                                .or(functions.col("avg_loss").isNull()),
                        functions.lit(null).cast(DataTypes.DoubleType)   // chưa đủ dữ liệu
                ).when(
                        functions.col("avg_loss").equalTo(0.0)
                                .and(functions.col("avg_gain").equalTo(0.0)),
                        functions.lit(50.0)
                ).when(
                        functions.col("avg_loss").equalTo(0.0),
                        functions.lit(100.0)
                ).otherwise(
                        functions.lit(100.0).minus(
                                functions.lit(100.0).divide(
                                        functions.lit(1.0).plus(
                                                functions.col("avg_gain").divide(functions.col("avg_loss"))
                                        )
                                )
                        )
                )
        );

        // ── 8. Tính SMA(200) ──────────────────────────────────────────────────
        WindowSpec smaWindow = Window
                .partitionBy("ticker")
                .orderBy("time")
                .rowsBetween(-(SMA_PERIOD - 1), 0);

        Dataset<Row> withSma = withRsi
                .withColumn("sma_200_raw", functions.avg(functions.col("close")).over(smaWindow))
                .withColumn("sma_200",
                        functions.when(
                                functions.col("row_num").lt(SMA_PERIOD),
                                functions.lit(null).cast(DataTypes.DoubleType)
                        ).otherwise(functions.col("sma_200_raw"))
                );

        // ── 9. Chỉ giữ các cột output, thêm calculated_at ────────────────────
        Dataset<Row> result = withSma.select(
                functions.col("ticker"),
                functions.col("time"),
                functions.col("date_id"),
                functions.col("close").cast(DataTypes.createDecimalType(20, 2)),
                functions.round(functions.col("rsi_14"), 6).cast(DataTypes.DoubleType).as("rsi_14"),
                functions.round(functions.col("sma_200"), 6).cast(DataTypes.DoubleType).as("sma_200"),
                functions.current_timestamp().as("calculated_at")
        );

        long count = result.count();
        System.out.println("TechnicalIndicatorHistoricalProcessor: computed " + count
                + " rows. Writing to stock.technical_indicators_historical...");

        // ── 10. Ghi vào ClickHouse (Overwrite toàn bộ — đây là backfill) ─────
        clickhouseSink.write(result, "stock.technical_indicators_historical", SaveMode.Overwrite);

        System.out.println("TechnicalIndicatorHistoricalProcessor: done.");
    }
}
