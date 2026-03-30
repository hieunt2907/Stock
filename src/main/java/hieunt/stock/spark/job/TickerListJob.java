package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.TickerListProcessor;

public class TickerListJob {
    public static void main(String[] args) {
        System.out.println("Starting Spark Job: MinIO to Postgres (Ticker List)...");
        try {
            TickerListProcessor processor = new TickerListProcessor();
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
