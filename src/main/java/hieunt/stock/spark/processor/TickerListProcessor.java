package hieunt.stock.spark.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TickerListProcessor extends AbstractMinioToPostgresProcessor {
    public TickerListProcessor(String partitionPath) {
        super("ticker_list", "stock.ticker_list", "ticker_list", partitionPath);
    }

    @Override
    protected String[] getBusinessKeys() {
        return new String[]{"ticker"}; // ticker_list chỉ có business key là ticker
    }

    @Override
    protected Dataset<Row> transform(Dataset<Row> df) {
        // Ticker list mainly strings, already trimmed in the abstract processor
        return df;
    }
}
