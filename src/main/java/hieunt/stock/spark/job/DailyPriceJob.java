package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.DailyPriceProcessor;

public class DailyPriceJob {
    public static void main(String[] args) {
        System.out.println("Starting Spark Job: MinIO to Postgres (Daily Price)...");
        try {
            DailyPriceProcessor processor = new DailyPriceProcessor();
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
