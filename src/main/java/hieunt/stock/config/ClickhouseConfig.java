package hieunt.stock.config;

import io.github.cdimascio.dotenv.Dotenv;

public final class ClickhouseConfig {
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    public static final String HOST = dotenv.get("CH_HOST");
    public static final String PORT = dotenv.get("CH_PORT");
    public static final String USER = dotenv.get("CH_USER");
    public static final String PASSWORD = dotenv.get("CH_PASSWORD");
    public static final String DATABASE = dotenv.get("CH_DATABASE");
    public static final int MAX_CONNECTION = Integer.parseInt(dotenv.get("CH_MAX_CONNECTION"));
    public static final int MAX_IDLE_CONNECTION = Integer.parseInt(dotenv.get("CH_MAX_IDLE_CONNECTION"));
    public static final int MAX_LIFETIME_CONNECTION = Integer.parseInt(dotenv.get("CH_MAX_LIFETIME_CONNECTION"));

    public static String getJdbcUrl() {
        return String.format("jdbc:clickhouse://%s:%s/%s", HOST, PORT, DATABASE);
    }
}
