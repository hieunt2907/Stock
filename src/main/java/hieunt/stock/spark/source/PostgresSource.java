package hieunt.stock.spark.source;

import hieunt.stock.config.PostgresConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class PostgresSource implements BaseSource {
    private final SparkSession spark;

    public PostgresSource(SparkSession spark) {
        this.spark = spark;
    }

    @Override
    public Dataset<Row> read(String tableName) {
        Properties properties = new Properties();
        properties.setProperty("user", PostgresConfig.DB_USERNAME);
        properties.setProperty("password", PostgresConfig.DB_PASSWORD);
        properties.setProperty("driver", "org.postgresql.Driver");

        return spark.read()
                .jdbc(PostgresConfig.DB_URL, tableName, properties);
    }
}
