package hieunt.stock.spark.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class CompanyMetadataProcessor extends AbstractMinioToPostgresProcessor {
    public CompanyMetadataProcessor() {
        super("company_metadata", "stock.company_metadata", "company_metadata");
    }

    @Override
    protected Dataset<Row> transform(Dataset<Row> df) {
        return df
                .withColumn("charter_capital",
                        functions.col("charter_capital").cast(DataTypes.createDecimalType(20, 2)))
                .withColumn("number_of_employees",
                        functions.col("number_of_employees").cast(DataTypes.LongType))
                .withColumn("par_value", functions.col("par_value").cast(DataTypes.LongType))
                .withColumn("listing_price", functions.col("listing_price").cast(DataTypes.LongType))
                .withColumn("listed_volume", functions.col("listed_volume").cast(DataTypes.LongType))
                .withColumn("free_float_percentage",
                        functions.col("free_float_percentage").cast(DataTypes.createDecimalType(20, 2)))
                .withColumn("free_float", functions.col("free_float").cast(DataTypes.createDecimalType(20, 2)))
                .withColumn("outstanding_shares", functions.col("outstanding_shares").cast(DataTypes.LongType))
                // Date parsing: some are dd/MM/yyyy and some might be ISO
                .withColumn("as_of_date", functions.to_timestamp(functions.col("as_of_date")))
                .withColumn("founded_date", parseDate(functions.col("founded_date")))
                .withColumn("listing_date", parseDate(functions.col("listing_date")));
    }
}
