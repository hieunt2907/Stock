package hieunt.stock.config;

import io.github.cdimascio.dotenv.Dotenv;

public final class MinIOConfig {
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    public static final String ENDPOINT = dotenv.get("MINIO_ENDPOINT");
    public static final String ACCESS_KEY = dotenv.get("MINIO_ACCESS_KEY");
    public static final String SECRET_KEY = dotenv.get("MINIO_SECRET_KEY");
    public static final String BUCKET = dotenv.get("MINIO_BUCKET");
    public static final String REGION = dotenv.get("MINIO_REGION");
    public static final boolean SECURE = Boolean.parseBoolean(dotenv.get("MINIO_SECURE"));
    public static final boolean PATH_STYLE = Boolean.parseBoolean(dotenv.get("MINIO_PATH_STYLE"));
}
