package hieunt.stock.kafka.consumer;

import hieunt.stock.kafka.AbstractKafkaConsumer;
import hieunt.stock.kafka.MinIOSink;

public class IntradayPriceConsumer extends AbstractKafkaConsumer {
    public IntradayPriceConsumer(MinIOSink minioSink) {
        super(minioSink, "intraday_price");
    }
}
