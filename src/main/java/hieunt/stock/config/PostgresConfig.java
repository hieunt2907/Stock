package hieunt.stock.config;

import io.github.cdimascio.dotenv.Dotenv;

public final class PostgresConfig {
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    public static final String DB_URL = dotenv.get("DB_URL");
    public static final String DB_USERNAME = dotenv.get("DB_USERNAME");
    public static final String DB_PASSWORD = dotenv.get("DB_PASSWORD");
}
