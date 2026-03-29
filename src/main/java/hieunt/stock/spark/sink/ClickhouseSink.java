package hieunt.stock.spark.sink;

import hieunt.stock.config.ClickhouseConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

public class ClickhouseSink implements BaseSink {
    
    public ClickhouseSink() {}

    @Override
    public void write(Dataset<Row> df, String tableName, SaveMode saveMode) {
        Properties properties = new Properties();
        properties.setProperty("user", ClickhouseConfig.USER);
        properties.setProperty("password", ClickhouseConfig.PASSWORD);
        properties.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver");
        
        // Add any additional ClickHouse specific JDBC properties if needed
        properties.setProperty("batch_size", "100000");
        properties.setProperty("rewrite_batch_insert", "true");
        properties.setProperty("isolationLevel", "NONE");

        df.write()
                .mode(saveMode)
                .jdbc(ClickhouseConfig.getJdbcUrl(), tableName, properties);
    }
}

