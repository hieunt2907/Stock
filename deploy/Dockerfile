# ===== STAGE 1: BUILD (Maven 3.8 + JDK 11) =====
FROM maven:3.8.7-eclipse-temurin-11-alpine AS builder
WORKDIR /app
# Cache dependencies trước khi copy source
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
# Chỉ build kafka-consumer JAR
RUN mvn clean package -DskipTests

# ===== STAGE 2: RUNTIME (JRE 11 slim) =====
FROM eclipse-temurin:11-jre-alpine
WORKDIR /app

# Copy ONLY kafka-consumer JAR từ builder stage
COPY --from=builder /app/target/kafka-consumer-1.0-SNAPSHOT.jar /app/kafka-consumer.jar

# Copy .env để MinIOSink & KafkaConsumer đọc config
COPY .env /app/.env

CMD ["java", "-jar", "/app/kafka-consumer.jar"]