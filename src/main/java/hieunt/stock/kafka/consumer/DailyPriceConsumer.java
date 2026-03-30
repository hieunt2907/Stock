package hieunt.stock.kafka.consumer;

import hieunt.stock.kafka.AbstractKafkaConsumer;
import hieunt.stock.kafka.MinIOSink;

public class DailyPriceConsumer extends AbstractKafkaConsumer {
    public DailyPriceConsumer(MinIOSink minioSink) {
        super(minioSink, "daily_price");
    }
}
