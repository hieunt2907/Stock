package hieunt.stock.kafka;

import hieunt.stock.config.MinIOConfig;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class MinIOSink {
    private static final Logger logger = LoggerFactory.getLogger(MinIOSink.class);
    private final MinioClient minioClient;

    public MinIOSink() {
        this.minioClient = MinioClient.builder()
                .endpoint(MinIOConfig.ENDPOINT)
                .credentials(MinIOConfig.ACCESS_KEY, MinIOConfig.SECRET_KEY)
                .build();
        initMinioBucket();
    }

    private void initMinioBucket() {
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(MinIOConfig.BUCKET).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(MinIOConfig.BUCKET).build());
                logger.info("Đã tạo MinIO bucket: {}", MinIOConfig.BUCKET);
            }
        } catch (Exception e) {
            logger.error("Lỗi khi kết nối hoặc khởi tạo MinIO bucket", e);
            throw new RuntimeException("Could not verify or create MinIO bucket", e);
        }
    }

    public void upload(String objectName, String jsonContent) {
        try {
            byte[] data = jsonContent.getBytes(StandardCharsets.UTF_8);
            try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(MinIOConfig.BUCKET)
                                .object(objectName)
                                .stream(bais, bais.available(), -1)
                                .contentType("application/json")
                                .build()
                );
            }
            logger.info("Đã upload dữ liệu thành công lên MinIO: {}", objectName);
        } catch (Exception e) {
            logger.error("Lỗi khi upload đối tượng {} lên MinIO", objectName, e);
        }
    }
}
