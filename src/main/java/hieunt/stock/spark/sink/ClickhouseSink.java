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
        properties.setProperty("batchsize", "100000");
        properties.setProperty("rewrite_batch_insert", "true");
        properties.setProperty("socket_timeout", "300000");
        properties.setProperty("connection_timeout", "300000");
        properties.setProperty("compress", "0");
        properties.setProperty("isolationLevel", "NONE");

        df.write()
                .mode(saveMode)
                .jdbc(ClickhouseConfig.getJdbcUrl(), tableName, properties);
    }
}