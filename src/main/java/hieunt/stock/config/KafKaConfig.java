package hieunt.stock.config;

import io.github.cdimascio.dotenv.Dotenv;

public final class KafKaConfig {
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    public static final String ADDRESS = dotenv.get("KAFKA_ADDRESS");
    public static final String PRODUCER_TOPIC = dotenv.get("KAFKA_PRODUCER_TOPIC");
    public static final String CONSUMER_TOPIC = dotenv.get("KAFKA_CONSUMER_TOPIC");
    public static final String CONSUMER_GROUP_ID = dotenv.get("KAFKA_CONSUMER_GROUP_ID");
    public static final int CONSUMER_CONCURRENT_THREAD = Integer.parseInt(dotenv.get("KAFKA_CONSUMER_CONCURRENT_THREAD"));
    public static final boolean CONSUMER_BATCH = Boolean.parseBoolean(dotenv.get("KAFKA_CONSUMER_BATCH"));
    public static final int CONSUMER_NUMBER_OF_MESSAGE_IN_BATCH = Integer.parseInt(dotenv.get("KAFKA_CONSUMER_NUMBER_OF_MESSAGE_IN_BATCH"));
    public static final int CONSUMER_MAX_POLL_INTERVAL_MS = Integer.parseInt(dotenv.get("KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS"));
    public static final int CONSUMER_TIMEOUT_MS = Integer.parseInt(dotenv.get("KAFKA_CONSUMER_TIMEOUT_MS"));
    public static final int CONSUMER_RECONNECT_BACKOFF_MS = Integer.parseInt(dotenv.get("KAFKA_CONSUMER_RECONNECT_BACKOFF_MS"));
    public static final int CONSUMER_RECONNECT_BACKOFF_MAX_MS = Integer.parseInt(dotenv.get("KAFKA_CONSUMER_RECONNECT_BACKOFF_MAX_MS"));
    public static final int CONSUMER_HEARTBEAT_INTERVAL_MS = Integer.parseInt(dotenv.get("KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS"));
    public static final int SHUTDOWN_WAIT_TIME_MAX = Integer.parseInt(dotenv.get("KAFKA_SHUTDOWN_WAIT_TIME_MAX"));
}
