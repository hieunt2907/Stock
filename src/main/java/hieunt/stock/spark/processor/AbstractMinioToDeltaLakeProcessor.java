package hieunt.stock.spark.processor;

import hieunt.stock.config.MinIOConfig;
import hieunt.stock.spark.Spark;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public abstract class AbstractMinioToDeltaLakeProcessor implements BaseProcessor {
    protected final String type;
    protected final String tableName;
    protected final String folderPath;

    /**
     * Partition path do Airflow truyền vào, format: yyyy-MM-dd_HH
     * Ví dụ: "2026-03-31_17" → đọc folder daily_price/2026-03-31_17/
     * Nếu null → fallback đọc toàn bộ folder (dành cho backfill thủ công).
     */
    protected final String partitionPath;

    public AbstractMinioToDeltaLakeProcessor(String type, String tableName, String folderPath, String partitionPath) {
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

                // Loại bỏ duplicate theo business key
                Dataset<Row> dedupedData = transformedData.dropDuplicates(getBusinessKeys());

                // Drop technical/temporary columns if they exist
                Dataset<Row> finalData = dedupedData;
                String[] technicalColumns = { "type", "kafka_ingested_at" };
                for (String colName : technicalColumns) {
                    if (java.util.Arrays.asList(finalData.columns()).contains(colName)) {
                        finalData = finalData.drop(colName);
                    }
                }

                String outPath = String.format("s3a://%s/delta_lake/%s/", MinIOConfig.BUCKET, tableName);

                System.out.println("Writing to Delta Lake table: " + outPath
                        + " | Partition: " + (partitionPath != null ? partitionPath : "ALL")
                        + " | Rows: " + finalData.count());
                        
                finalData.write()
                        .mode(SaveMode.Append)
                        .format("delta")
                        .save(outPath);
            } else {
                System.out.println("No data found for " + type + " in path: " + fullPath);
            }
        } catch (Exception e) {
            System.out.println("Error processing " + type + " from " + fullPath + ": " + e.getMessage());
        }
    }

    protected abstract Dataset<Row> transform(Dataset<Row> df);

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
