package hieunt.stock.spark.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class DailyPriceProcessor extends AbstractMinioToPostgresProcessor {
    public DailyPriceProcessor(String partitionPath) {
        super("daily_price", "stock.daily_price", "daily_price", partitionPath);
    }

    @Override
    protected Dataset<Row> transform(Dataset<Row> df) {
        return df.withColumn("time", functions.to_timestamp(functions.col("time")))
                .withColumn("open", functions.col("open").cast(DataTypes.createDecimalType(20, 2)))
                .withColumn("high", functions.col("high").cast(DataTypes.createDecimalType(20, 2)))
                .withColumn("low", functions.col("low").cast(DataTypes.createDecimalType(20, 2)))
                .withColumn("close", functions.col("close").cast(DataTypes.createDecimalType(20, 2)))
                .withColumn("volume", functions.col("volume").cast(DataTypes.LongType));
    }
}
