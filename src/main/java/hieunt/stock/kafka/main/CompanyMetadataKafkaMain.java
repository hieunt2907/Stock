package hieunt.stock.kafka.main;

import hieunt.stock.kafka.consumer.CompanyMetadataConsumer;
import hieunt.stock.kafka.MinIOSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompanyMetadataKafkaMain {
    private static final Logger logger = LoggerFactory.getLogger(CompanyMetadataKafkaMain.class);

    public static void main(String[] args) {
        logger.info("Khởi tạo Kafka Consumer cho Company Metadata...");
        try {
            MinIOSink minioSink = new MinIOSink();
            CompanyMetadataConsumer consumer = new CompanyMetadataConsumer(minioSink);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Đang đóng CompanyMetadataConsumer...");
                consumer.stop();
            }));

            consumer.start();
        } catch (Exception e) {
            logger.error("Lỗi crash CompanyMetadataKafkaMain", e);
        }
    }
}
