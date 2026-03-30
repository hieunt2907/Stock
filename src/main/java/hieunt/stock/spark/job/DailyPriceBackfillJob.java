package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.DailyPriceBackfillProcessor;

public class DailyPriceBackfillJob {
    public static void main(String[] args) {
        System.out.println("Starting Spark Job: MinIO to Postgres (Daily Price Backfill)...");
        try {
            DailyPriceBackfillProcessor processor = new DailyPriceBackfillProcessor();
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
