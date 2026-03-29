package hieunt.stock.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMain {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMain.class);

    public static void main(String[] args) {
        logger.info("Đang khởi tạo ứng dụng Kafka to MinIO...");

        try {
            // Bước 1: Khởi tạo MinIOSink để thiết lập kết nối và check bucket
            MinIOSink minioSink = new MinIOSink();

            // Bước 2: Khởi tạo KafkaConsumer và đưa MinIOSink vào qua constructor (Dependency Injection)
            KafkaConsumer consumer = new KafkaConsumer(minioSink);

            // Đăng ký Shutdown Hook để đóng KafkaConsumer an toàn khi tắt app (nhấn Ctrl+C)
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Nhận tín hiệu tắt ứng dụng. Đang đóng kết nối Consumer...");
                consumer.stop();
                // Đóng kết nối thread đang chạy để nó gọi block finally
            }));

            // Bước 3: Bắt đầu quá trình consume message (blocking loop)
            consumer.start();

        } catch (Exception e) {
            logger.error("Lỗi nghiêm trọng, ứng dụng bị crash", e);
        }
    }
}
