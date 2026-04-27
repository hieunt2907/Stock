package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.IntradayTickStreamProcessor;

public class IntradayTickStreamJob {
    public static void main(String[] args) {
        System.out.println("Starting Spark Structured Streaming: Kafka → ClickHouse (intraday_tick)");
        try {
            new IntradayTickStreamProcessor().process();
        } catch (Exception e) {
            System.err.println("Streaming job failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
