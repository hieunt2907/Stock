package hieunt.stock.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import hieunt.stock.config.KafKaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final DateTimeFormatter dtfHour = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH");
    private static final DateTimeFormatter dtfMinute = DateTimeFormatter.ofPattern("mm-ss-SSS");

    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;
    private final MinIOSink minioSink;
    private final ObjectMapper objectMapper;
    private volatile boolean running;

    public KafkaConsumer(MinIOSink minioSink) {
        this.running = true;
        this.minioSink = minioSink;
        this.objectMapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafKaConfig.ADDRESS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafKaConfig.CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, KafKaConfig.CONSUMER_MAX_POLL_INTERVAL_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, KafKaConfig.CONSUMER_TIMEOUT_MS);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, KafKaConfig.CONSUMER_RECONNECT_BACKOFF_MS);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, KafKaConfig.CONSUMER_RECONNECT_BACKOFF_MAX_MS);

        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(KafKaConfig.CONSUMER_TOPIC));
    }

    public void start() {
        logger.info("Kafka consumer bắt đầu đọc từ topic: {}", KafKaConfig.CONSUMER_TOPIC);

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } catch (Exception e) {
            logger.error("Lỗi trong quá trình consume Kafka", e);
        } finally {
            close();
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        String jsonContent = record.value();
        logger.debug("Đang xử lý record: {}", jsonContent);

        String folder = "raw_data";
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonContent);
            if (jsonNode.has("type")) {
                folder = jsonNode.get("type").asText();
            }
        } catch (Exception e) {
            logger.warn("Không thể parse JSON để lấy folder, sử dụng folder mặc định: {}", folder);
        }

        LocalDateTime now = LocalDateTime.now();
        String objectName = String.format("%s/%s/%s/%s_%s.json",
                KafKaConfig.CONSUMER_TOPIC,
                folder,
                dtfHour.format(now),
                dtfMinute.format(now),
                UUID.randomUUID().toString().substring(0, 8));

        minioSink.upload(objectName, jsonContent);
    }

    public void stop() {
        this.running = false;
        logger.info("Kafka consumer được yêu cầu dừng.");
    }

    public void close() {
        if (consumer != null) {
            consumer.close();
            logger.info("Đã đóng kết nối Kafka consumer an toàn.");
        }
    }
}
