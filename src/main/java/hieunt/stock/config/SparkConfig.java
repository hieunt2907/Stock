package hieunt.stock.config;

import io.github.cdimascio.dotenv.Dotenv;

public final class SparkConfig {
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    public static final String MASTER = dotenv.get("SPARK_MASTER");
    public static final String APP_NAME = dotenv.get("SPARK_APP_NAME");
    public static final String EXECUTOR_MEMORY = dotenv.get("SPARK_EXECUTOR_MEMORY");
    public static final String EXECUTOR_CORES = dotenv.get("SPARK_EXECUTOR_CORES");
    public static final String EXECUTOR_INSTANCES = dotenv.get("SPARK_EXECUTOR_INSTANCES");
    public static final String DRIVER_MEMORY = dotenv.get("SPARK_DRIVER_MEMORY");
    public static final String DRIVER_CORES = dotenv.get("SPARK_DRIVER_CORES");
    public static final String SQL_SHUFFLE_PARTITIONS = dotenv.get("SPARK_SQL_SHUFFLE_PARTITIONS");
    public static final String MEMORY_FRACTION = dotenv.get("SPARK_MEMORY_FRACTION");
    public static final String MEMORY_STORAGE_FRACTION = dotenv.get("SPARK_MEMORY_STORAGE_FRACTION");
    public static final boolean MEMORY_OFF_HEAP_ENABLED = Boolean
            .parseBoolean(dotenv.get("SPARK_MEMORY_OFF_HEAP_ENABLED"));
    public static final String MEMORY_OFF_HEAP_SIZE = dotenv.get("SPARK_MEMORY_OFF_HEAP_SIZE");
}
