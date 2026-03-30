package hieunt.stock.kafka.main;

import hieunt.stock.kafka.consumer.DailyPriceBackfillConsumer;
import hieunt.stock.kafka.MinIOSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DailyPriceBackfillKafkaMain {
    private static final Logger logger = LoggerFactory.getLogger(DailyPriceBackfillKafkaMain.class);

    public static void main(String[] args) {
        logger.info("Khởi tạo Kafka Consumer cho Daily Price Backfill...");
        try {
            MinIOSink minioSink = new MinIOSink();
            DailyPriceBackfillConsumer consumer = new DailyPriceBackfillConsumer(minioSink);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Đang đóng DailyPriceBackfillConsumer...");
                consumer.stop();
            }));

            consumer.start();
        } catch (Exception e) {
            logger.error("Lỗi crash DailyPriceBackfillKafkaMain", e);
        }
    }
}
