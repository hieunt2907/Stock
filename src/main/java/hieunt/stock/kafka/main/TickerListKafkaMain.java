package hieunt.stock.kafka.main;

import hieunt.stock.kafka.consumer.TickerListConsumer;
import hieunt.stock.kafka.MinIOSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TickerListKafkaMain {
    private static final Logger logger = LoggerFactory.getLogger(TickerListKafkaMain.class);

    public static void main(String[] args) {
        logger.info("Khởi tạo Kafka Consumer cho Ticker List...");
        try {
            MinIOSink minioSink = new MinIOSink();
            TickerListConsumer consumer = new TickerListConsumer(minioSink);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Đang đóng TickerListConsumer...");
                consumer.stop();
            }));

            consumer.start();
        } catch (Exception e) {
            logger.error("Lỗi crash TickerListKafkaMain", e);
        }
    }
}
