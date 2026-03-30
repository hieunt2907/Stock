package hieunt.stock.spark.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TickerListProcessor extends AbstractMinioToPostgresProcessor {
    public TickerListProcessor() {
        super("ticker_list", "stock.ticker_list", "ticker_list");
    }

    @Override
    protected Dataset<Row> transform(Dataset<Row> df) {
        // Ticker list mainly strings, already trimmed in the abstract processor
        return df;
    }
}
