package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.TickerListProcessor;

public class TickerListJob {
    public static void main(String[] args) {
        String partitionPath = (args != null && args.length > 0) ? args[0] : null;
        System.out.println("Starting Spark Job: MinIO to Postgres (Ticker List) | Partition: " + partitionPath);
        try {
            TickerListProcessor processor = new TickerListProcessor(partitionPath);
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
