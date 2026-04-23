package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.FinancialRatioProcessor;


public class FinancialRatioJob {
    public static void main(String[] args) {
        String partitionPath = (args != null && args.length > 0) ? args[0] : null;
        System.out.println(
                "Starting Spark Job: MinIO → ClickHouse (Financial Ratio) | Partition: " + partitionPath
        );
        try {
            FinancialRatioProcessor processor = new FinancialRatioProcessor(partitionPath);
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
