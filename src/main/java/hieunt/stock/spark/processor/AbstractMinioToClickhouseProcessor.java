package hieunt.stock.spark.processor;

import hieunt.stock.config.MinIOConfig;
import hieunt.stock.spark.Spark;
import hieunt.stock.spark.sink.ClickhouseSink;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;

public abstract class AbstractMinioToClickhouseProcessor implements BaseProcessor {
    protected final ClickhouseSink clickhouseSink;
    protected final String type;
    protected final String tableName;
    protected final String folderPath;

    /**
     * Partition path do Airflow truyền vào, format: yyyy-MM-dd_HH
     * Ví dụ: "2026-03-31_17" → đọc folder daily_price/2026-03-31_17/
     * Nếu null → fallback đọc toàn bộ folder (dành cho backfill thủ công).
     */
    protected final String partitionPath;

    public AbstractMinioToClickhouseProcessor(String type, String tableName, String folderPath, String partitionPath) {
        this.clickhouseSink = new ClickhouseSink();
        this.type = type;
        this.tableName = tableName;
        this.folderPath = folderPath;
        this.partitionPath = partitionPath;
    }

    /**
     * Trả về danh sách business key dùng để dropDuplicates.
     * Subclass override nếu cần. Mặc định là ["ticker", "time"].
     */
    protected String[] getBusinessKeys() {
        return new String[] { "ticker", "time" };
    }

    @Override
    public void process() {
        SparkSession spark = Spark.getSparkSession();

        // Nếu Airflow truyền partitionPath (e.g. "2026-03-31_17") → chỉ đọc folder đó.
        // Nếu không → đọc toàn bộ (fallback cho backfill thủ công).
        String fullPath;
        if (partitionPath != null && !partitionPath.isEmpty()) {
            fullPath = String.format("s3a://%s/%s/%s/", MinIOConfig.BUCKET, folderPath, partitionPath);
        } else {
            fullPath = String.format("s3a://%s/%s/", MinIOConfig.BUCKET, folderPath);
        }

        System.out.println("Processing " + type + " from path: " + fullPath);

        try {
            Dataset<Row> df = spark.read()
                    .option("recursiveFileLookup", "true")
                    .json(fullPath);

            if (df != null && !df.isEmpty()) {
                Dataset<Row> normalizedData = trimAllStringColumns(df);
                Dataset<Row> transformedData = transform(normalizedData);

                // Xử lý giá trị thiếu (null):
                // - Số nguyên / Decimal / Float / Double → 0 / 0.0
                // - String → " " (single space)
                // - Các kiểu khác (Timestamp, Date, ...) → giữ nguyên null
                Dataset<Row> filledData = fillMissingValues(transformedData);

                // Loại bỏ duplicate theo business key
                Dataset<Row> dedupedData = filledData.dropDuplicates(getBusinessKeys());

                // Bỏ các cột kỹ thuật / tạm thời nếu tồn tại
                Dataset<Row> finalData = dedupedData;
                String[] technicalColumns = { "type", "kafka_ingested_at" };
                for (String colName : technicalColumns) {
                    if (Arrays.asList(finalData.columns()).contains(colName)) {
                        finalData = finalData.drop(colName);
                    }
                }

                System.out.println("Writing to ClickHouse table: " + tableName
                        + " | Partition: " + (partitionPath != null ? partitionPath : "ALL")
                        + " | Rows: " + finalData.count());
                clickhouseSink.write(finalData, tableName, SaveMode.Append);
            } else {
                System.out.println("No data found for " + type + " in path: " + fullPath);
            }
        } catch (Exception e) {
            System.out.println("Error processing " + type + " from " + fullPath + ": " + e.getMessage());
        }
    }

    protected abstract Dataset<Row> transform(Dataset<Row> df);

    /**
     * Xử lý giá trị thiếu (null) theo kiểu dữ liệu:
     * - IntegerType / LongType / ShortType / ByteType → fill 0
     * - FloatType / DoubleType / DecimalType → fill 0.0
     * - StringType → fill " "
     * - Các kiểu khác (Timestamp, Date, Boolean, ...) → giữ nguyên
     */
    protected Dataset<Row> fillMissingValues(Dataset<Row> df) {
        Dataset<Row> result = df;
        for (StructField field : df.schema().fields()) {
            String colName = field.name();
            DataType dataType = field.dataType();

            if (dataType.equals(DataTypes.IntegerType)
                    || dataType.equals(DataTypes.LongType)
                    || dataType.equals(DataTypes.ShortType)
                    || dataType.equals(DataTypes.ByteType)) {
                result = result.withColumn(colName,
                        functions.coalesce(functions.col(colName), functions.lit(0).cast(dataType)));

            } else if (dataType.equals(DataTypes.FloatType)
                    || dataType.equals(DataTypes.DoubleType)
                    || dataType instanceof DecimalType) {
                result = result.withColumn(colName,
                        functions.coalesce(functions.col(colName), functions.lit(0.0).cast(dataType)));

            } else if (dataType.equals(DataTypes.StringType)) {
                result = result.withColumn(colName,
                        functions.coalesce(functions.col(colName), functions.lit(" ")));

            } else if (dataType.equals(DataTypes.DateType)) {
                result = result.withColumn(colName,
                        functions.coalesce(functions.col(colName),
                                functions.lit("1970-01-01").cast(DataTypes.DateType)));

            } else if (dataType.equals(DataTypes.TimestampType)) {
                result = result.withColumn(colName,
                        functions.coalesce(functions.col(colName),
                                functions.lit("1970-01-01 00:00:00").cast(DataTypes.TimestampType)));

            } else if (dataType.equals(DataTypes.BooleanType)) {
                result = result.withColumn(colName,
                        functions.coalesce(functions.col(colName), functions.lit(false).cast(DataTypes.BooleanType)));
            }
        }
        return result;
    }

    protected Dataset<Row> trimAllStringColumns(Dataset<Row> df) {
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

    protected Column parseDate(Column col) {
        return functions.coalesce(
                functions.to_date(col, "dd/MM/yyyy"),
                functions.to_date(col, "yyyy-MM-dd"),
                functions.to_date(col)).cast(DataTypes.StringType);
    }
}
