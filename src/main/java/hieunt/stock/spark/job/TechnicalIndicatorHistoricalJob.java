package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.TechnicalIndicatorHistoricalProcessor;

/**
 * TechnicalIndicatorHistoricalJob
 *
 * Entry point cho Spark job tính RSI(14) và SMA(200) trên toàn bộ
 * lịch sử giá cổ phiếu (backfill).
 *
 * Không nhận args vì đây là full-scan, không phải incremental.
 *
 * Submit:
 *   spark-submit --class hieunt.stock.spark.job.TechnicalIndicatorHistoricalJob \
 *     stock.jar
 */
public class TechnicalIndicatorHistoricalJob {
    public static void main(String[] args) {
        System.out.println("Starting Spark Job: Technical Indicators Historical (RSI-14, SMA-200) [Backfill]");
        try {
            TechnicalIndicatorHistoricalProcessor processor = new TechnicalIndicatorHistoricalProcessor();
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
