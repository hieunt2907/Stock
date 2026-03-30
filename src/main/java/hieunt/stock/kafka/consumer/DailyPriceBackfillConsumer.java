package hieunt.stock.kafka.consumer;

import hieunt.stock.kafka.AbstractKafkaConsumer;
import hieunt.stock.kafka.MinIOSink;

public class DailyPriceBackfillConsumer extends AbstractKafkaConsumer {
    public DailyPriceBackfillConsumer(MinIOSink minioSink) {
        super(minioSink, "daily_price_backfill");
    }
}
