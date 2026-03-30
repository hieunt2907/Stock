package hieunt.stock.kafka.consumer;

import hieunt.stock.kafka.AbstractKafkaConsumer;
import hieunt.stock.kafka.MinIOSink;

public class CompanyMetadataConsumer extends AbstractKafkaConsumer {
    public CompanyMetadataConsumer(MinIOSink minioSink) {
        super(minioSink, "company_metadata");
    }
}
