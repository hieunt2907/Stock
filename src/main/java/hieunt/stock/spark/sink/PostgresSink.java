package hieunt.stock.spark.sink;

import hieunt.stock.config.PostgresConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

public class PostgresSink implements BaseSink {
    
    public PostgresSink() {}

    @Override
    public void write(Dataset<Row> df, String tableName, SaveMode saveMode) {
        Properties properties = new Properties();
        properties.setProperty("user", PostgresConfig.DB_USERNAME);
        properties.setProperty("password", PostgresConfig.DB_PASSWORD);
        properties.setProperty("driver", "org.postgresql.Driver");
        
        // Performance tuning
        properties.setProperty("batchsize", "10000");
        properties.setProperty("rewriteBatchedStatements", "true");

        df.write()
                .mode(saveMode)
                .jdbc(PostgresConfig.DB_URL, tableName, properties);
    }
}
