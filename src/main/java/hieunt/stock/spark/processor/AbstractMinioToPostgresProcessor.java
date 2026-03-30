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

public abstract class AbstractMinioToPostgresProcessor implements BaseProcessor {
    protected final PostgresSink postgresSink;
    protected final String type;
    protected final String tableName;
    protected final String folderPath;

    public AbstractMinioToPostgresProcessor(String type, String tableName, String folderPath) {
        this.postgresSink = new PostgresSink();
        this.type = type;
        this.tableName = tableName;
        this.folderPath = folderPath;
    }

    @Override
    public void process() {
        SparkSession spark = Spark.getSparkSession();
        String fullPath = String.format("s3a://%s/%s/", MinIOConfig.BUCKET, folderPath);
        
        System.out.println("Processing " + type + " from path: " + fullPath);

        try {
            Dataset<Row> df = spark.read()
                    .option("recursiveFileLookup", "true")
                    .json(fullPath);

            if (df != null && !df.isEmpty()) {
                Dataset<Row> normalizedData = trimAllStringColumns(df);
                Dataset<Row> transformedData = transform(normalizedData);
                
                // Unique version for this batch run in readable format (yyyyMMddHHmm)
                java.time.format.DateTimeFormatter vFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmm");
                long batchVersion = Long.parseLong(java.time.LocalDateTime.now().format(vFormatter));
                
                // Add audit columns
                Dataset<Row> auditData = transformedData
                        .withColumn("created_at", functions.current_timestamp())
                        .withColumn("version", functions.lit(batchVersion));

                // Drop technical/temporary columns if they exist
                Dataset<Row> finalData = auditData;
                String[] technicalColumns = {"type", "kafka_ingested_at"};
                for (String colName : technicalColumns) {
                    if (java.util.Arrays.asList(finalData.columns()).contains(colName)) {
                        finalData = finalData.drop(colName);
                    }
                }

                System.out.println("Writing to Postgres table: " + tableName + " - Batch Version: " + batchVersion + " (rows: " + finalData.count() + ")");
                postgresSink.write(finalData, tableName, SaveMode.Append);
            } else {
                System.out.println("No data found for " + type);
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
