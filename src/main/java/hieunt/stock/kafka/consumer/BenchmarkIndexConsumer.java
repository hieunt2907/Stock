package hieunt.stock.kafka.consumer;

import hieunt.stock.kafka.AbstractKafkaConsumer;
import hieunt.stock.kafka.MinIOSink;

public class BenchmarkIndexConsumer extends AbstractKafkaConsumer {
    public BenchmarkIndexConsumer(MinIOSink minioSink) {
        super(minioSink, "benchmark_index");
    }
}
