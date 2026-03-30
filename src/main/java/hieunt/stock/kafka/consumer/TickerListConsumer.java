package hieunt.stock.kafka.consumer;

import hieunt.stock.kafka.AbstractKafkaConsumer;
import hieunt.stock.kafka.MinIOSink;

public class TickerListConsumer extends AbstractKafkaConsumer {
    public TickerListConsumer(MinIOSink minioSink) {
        super(minioSink, "ticker_list");
    }
}
