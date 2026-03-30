package hieunt.stock.kafka.main;

import hieunt.stock.kafka.consumer.DailyPriceConsumer;
import hieunt.stock.kafka.MinIOSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DailyPriceKafkaMain {
    private static final Logger logger = LoggerFactory.getLogger(DailyPriceKafkaMain.class);

    public static void main(String[] args) {
        logger.info("Khởi tạo Kafka Consumer cho Daily Price...");
        try {
            MinIOSink minioSink = new MinIOSink();
            DailyPriceConsumer consumer = new DailyPriceConsumer(minioSink);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Đang đóng DailyPriceConsumer...");
                consumer.stop();
            }));

            consumer.start();
        } catch (Exception e) {
            logger.error("Lỗi crash DailyPriceKafkaMain", e);
        }
    }
}
