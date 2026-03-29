package hieunt.stock.spark.processor;

import hieunt.stock.config.MinIOConfig;
import hieunt.stock.spark.Spark;
import hieunt.stock.spark.sink.PostgresSink;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

public class MinioToPostgresProcessor implements BaseProcessor {
    private final PostgresSink postgresSink;

    public MinioToPostgresProcessor() {
        this.postgresSink = new PostgresSink();
    }

    @Override
    public void process() {
        SparkSession spark = Spark.getSparkSession();

        // Map type to its Postgres table and folder path in MinIO
        // Note: logs indicated nested 'stock' folder in the bucket
        Map<String, String[]> tableConfigs = new HashMap<>();
        // "type" -> ["tableName", "folderPath"]
        tableConfigs.put("ticker_list", new String[] { "stock.ticker_list", "stock/ticker_list" });
        tableConfigs.put("company_metadata", new String[] { "stock.company_metadata", "stock/company_metadata" });
        tableConfigs.put("benchmark_index", new String[] { "stock.benchmark_index", "stock/benchmark_index" });
        tableConfigs.put("daily_price_backfill",
                new String[] { "stock.daily_price_backfill", "stock/daily_price_backfill" });
        tableConfigs.put("daily_price", new String[] { "stock.daily_price", "stock/daily_price" });

        for (Map.Entry<String, String[]> entry : tableConfigs.entrySet()) {
            String type = entry.getKey();
            String tableName = entry.getValue()[0];
            String folderPath = entry.getValue()[1];

            // Build full path: s3a://[bucket]/[folderPath]/
            String fullPath = String.format("s3a://%s/%s/", MinIOConfig.BUCKET, folderPath);

            System.out.println("Processing " + type + " from path: " + fullPath);

            try {
                // Read JSON files for this specific topic
                Dataset<Row> df = spark.read()
                        .option("recursiveFileLookup", "true")
                        .json(fullPath);

                if (df != null && !df.isEmpty()) {
                    // 1. Trim string columns
                    Dataset<Row> normalizedData = trimAllStringColumns(df);

                    // 2. Perform type-specific normalization (Dates & Numbers)
                    Dataset<Row> transformedData = transformByType(normalizedData, type);

                    // 3. Drop 'type' column (not in Postgres) and write to Sink
                    Dataset<Row> finalData = transformedData.drop("type");

                    System.out.println("Writing to Postgres table: " + tableName + " (rows: " + finalData.count() + ")");
                    postgresSink.write(finalData, tableName, SaveMode.Append);
                }
            } catch (Exception e) {
                // Skip if path doesn't exist or other Spark errors (e.g. AnalysisException: Path does not exist)
                System.out.println("Path skip: " + fullPath + " (" + e.getMessage() + ")");
            }
        }
    }

    private Dataset<Row> trimAllStringColumns(Dataset<Row> df) {
        Column[] cols = new Column[df.columns().length];
        String[] columnNames = df.columns();

        for (int i = 0; i < columnNames.length; i++) {
            String colName = columnNames[i];
            if (df.schema().apply(colName).dataType().equals(DataTypes.StringType)) {
                cols[i] = functions.trim(functions.col(colName)).as(colName);
            } else {
                cols[i] = functions.col(colName);
            }
        }
        return df.select(cols);
    }

    private Dataset<Row> transformByType(Dataset<Row> df, String type) {
        switch (type) {
            case "company_metadata":
                return df
                        .withColumn("charter_capital",
                                functions.col("charter_capital").cast(DataTypes.createDecimalType(20, 2)))
                        .withColumn("number_of_employees",
                                functions.col("number_of_employees").cast(DataTypes.LongType))
                        .withColumn("par_value", functions.col("par_value").cast(DataTypes.LongType))
                        .withColumn("listing_price", functions.col("listing_price").cast(DataTypes.LongType))
                        .withColumn("listed_volume", functions.col("listed_volume").cast(DataTypes.LongType))
                        .withColumn("free_float_percentage",
                                functions.col("free_float_percentage").cast(DataTypes.createDecimalType(20, 2)))
                        .withColumn("free_float", functions.col("free_float").cast(DataTypes.createDecimalType(20, 2)))
                        .withColumn("outstanding_shares", functions.col("outstanding_shares").cast(DataTypes.LongType))
                        // Date parsing: some are dd/MM/yyyy and some might be ISO
                        .withColumn("as_of_date", functions.to_timestamp(functions.col("as_of_date")))
                        .withColumn("founded_date", parseDate(functions.col("founded_date")))
                        .withColumn("listing_date", parseDate(functions.col("listing_date")));

            case "benchmark_index":
            case "daily_price_backfill":
            case "daily_price":
                return df.withColumn("time", functions.to_timestamp(functions.col("time")))
                        .withColumn("open", functions.col("open").cast(DataTypes.createDecimalType(20, 2)))
                        .withColumn("high", functions.col("high").cast(DataTypes.createDecimalType(20, 2)))
                        .withColumn("low", functions.col("low").cast(DataTypes.createDecimalType(20, 2)))
                        .withColumn("close", functions.col("close").cast(DataTypes.createDecimalType(20, 2)))
                        .withColumn("volume", functions.col("volume").cast(DataTypes.LongType));

            case "ticker_list":
                // Ticker list mainly strings, already trimmed
                return df;

            default:
                return df;
        }
    }

    private Column parseDate(Column col) {
        // Try multiple formats or handle standard dd/MM/yyyy
        return functions.coalesce(
                functions.to_date(col, "dd/MM/yyyy"),
                functions.to_date(col, "yyyy-MM-dd"),
                functions.to_date(col)).cast(DataTypes.StringType); // Convert back to string for VARCHAR columns if
                                                                    // needed,
                                                                    // but here we keep as date if target is date,
                                                                    // however schema said VARCHAR(50) for
                                                                    // founded/listing_date
    }
}
