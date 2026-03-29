package hieunt.stock.spark.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public interface BaseSink {
    void write(Dataset<Row> df, String tableName, SaveMode saveMode);
}
