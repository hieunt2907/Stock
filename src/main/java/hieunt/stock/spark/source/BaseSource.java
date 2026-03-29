package hieunt.stock.spark.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface BaseSource {
    Dataset<Row> read(String pathOrTable);
}
