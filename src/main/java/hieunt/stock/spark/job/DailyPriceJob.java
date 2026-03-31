package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.DailyPriceProcessor;

public class DailyPriceJob {
    public static void main(String[] args) {
        // args[0]: partition path do Airflow truyền vào, ví dụ: "2026-03-31_17"
        // Nếu không có args → null → processor đọc toàn bộ folder (backfill thủ công)
        String partitionPath = (args != null && args.length > 0) ? args[0] : null;
        System.out.println("Starting Spark Job: MinIO to Postgres (Daily Price) | Partition: " + partitionPath);
        try {
            DailyPriceProcessor processor = new DailyPriceProcessor(partitionPath);
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
