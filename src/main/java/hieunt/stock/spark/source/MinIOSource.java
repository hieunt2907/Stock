package hieunt.stock.spark.source;

import hieunt.stock.config.MinIOConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MinIOSource implements BaseSource {
    private final SparkSession spark;

    public MinIOSource(SparkSession spark) {
        this.spark = spark;
    }

    @Override
    public Dataset<Row> read(String path) {
        String fullPath = String.format("s3a://%s/%s", MinIOConfig.BUCKET, path);
        
        return spark.read()
                .json(fullPath); 
    }
}
