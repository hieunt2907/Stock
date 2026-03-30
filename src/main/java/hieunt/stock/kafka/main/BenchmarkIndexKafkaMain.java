package hieunt.stock.kafka.main;

import hieunt.stock.kafka.consumer.BenchmarkIndexConsumer;
import hieunt.stock.kafka.MinIOSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkIndexKafkaMain {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkIndexKafkaMain.class);

    public static void main(String[] args) {
        logger.info("Khởi tạo Kafka Consumer cho Benchmark Index...");
        try {
            MinIOSink minioSink = new MinIOSink();
            BenchmarkIndexConsumer consumer = new BenchmarkIndexConsumer(minioSink);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Đang đóng BenchmarkIndexConsumer...");
                consumer.stop();
            }));

            consumer.start();
        } catch (Exception e) {
            logger.error("Lỗi crash BenchmarkIndexKafkaMain", e);
        }
    }
}
