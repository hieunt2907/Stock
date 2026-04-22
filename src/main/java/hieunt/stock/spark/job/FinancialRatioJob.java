package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.FinancialRatioProcessor;

/**
 * FinancialRatioJob
 * -----------------
 * Entry-point cho Spark job xử lý financial_ratio.
 *
 * Args:
 *   args[0] (optional) — partition path, vd: "2026-04-20_14"
 *                         Nếu không truyền → đọc toàn bộ folder (backfill).
 *
 * Chạy qua spark-submit:
 *   spark-submit --class hieunt.stock.spark.job.FinancialRatioJob spark-job.jar [partitionPath]
 */
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
