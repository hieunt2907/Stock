package hieunt.stock.kafka.main;

import hieunt.stock.kafka.consumer.IntradayPriceConsumer;
import hieunt.stock.kafka.MinIOSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntradayPriceKafkaMain {
    private static final Logger logger = LoggerFactory.getLogger(IntradayPriceKafkaMain.class);

    public static void main(String[] args) {
        logger.info("Khởi tạo Kafka Consumer cho Intraday Price...");
        try {
            MinIOSink minioSink = new MinIOSink();
            IntradayPriceConsumer consumer = new IntradayPriceConsumer(minioSink);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Đang đóng IntradayPriceConsumer...");
                consumer.stop();
            }));

            consumer.start();
        } catch (Exception e) {
            logger.error("Lỗi crash IntradayPriceKafkaMain", e);
        }
    }
}
