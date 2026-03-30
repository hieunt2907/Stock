package hieunt.stock.kafka;

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

public abstract class AbstractKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaConsumer.class);
    private static final DateTimeFormatter dtfHour = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH");
    private static final DateTimeFormatter dtfMinute = DateTimeFormatter.ofPattern("mm-ss-SSS");

    protected final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;
    protected final MinIOSink minioSink;
    protected final String topic;
    private volatile boolean running;

    public AbstractKafkaConsumer(MinIOSink minioSink, String topic) {
        this.running = true;
        this.minioSink = minioSink;
        this.topic = topic;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafKaConfig.ADDRESS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafKaConfig.CONSUMER_GROUP_ID + "_" + topic);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, KafKaConfig.CONSUMER_MAX_POLL_INTERVAL_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, KafKaConfig.CONSUMER_TIMEOUT_MS);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, KafKaConfig.CONSUMER_RECONNECT_BACKOFF_MS);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, KafKaConfig.CONSUMER_RECONNECT_BACKOFF_MAX_MS);

        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void start() {
        logger.info("Kafka consumer bắt đầu đọc từ topic: {}", topic);

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } catch (Exception e) {
            logger.error("Lỗi trong quá trình consume Kafka cho topic " + topic, e);
        } finally {
            close();
        }
    }

    protected void processRecord(ConsumerRecord<String, String> record) {
        String jsonContent = record.value();
        LocalDateTime now = LocalDateTime.now();
        
        // Enrich JSON with ingestion timestamp
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(jsonContent);
            if (root.isObject()) {
                ((com.fasterxml.jackson.databind.node.ObjectNode) root).put("kafka_ingested_at", now.toString());
                jsonContent = mapper.writeValueAsString(root);
            }
        } catch (Exception e) {
            logger.warn("Failed to enrich JSON with kafka_ingested_at for topic {}", topic);
        }

        // objectName follow standard: [topic]/[yyyy-MM-dd_HH]/[mm-ss-SSS]_[uuid].json
        String objectName = String.format("%s/%s/%s_%s.json",
                topic,
                dtfHour.format(now),
                dtfMinute.format(now),
                UUID.randomUUID().toString().substring(0, 8));

        minioSink.upload(objectName, jsonContent);
    }

    public void stop() {
        this.running = false;
        logger.info("Kafka consumer cho topic {} được yêu cầu dừng.", topic);
    }

    public void close() {
        if (consumer != null) {
            consumer.close();
            logger.info("Đã đóng kết nối Kafka consumer cho topic {} an toàn.", topic);
        }
    }
}
