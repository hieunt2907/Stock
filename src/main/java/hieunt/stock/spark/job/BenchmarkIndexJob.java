package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.BenchmarkIndexProcessor;

public class BenchmarkIndexJob {
    public static void main(String[] args) {
        System.out.println("Starting Spark Job: MinIO to Postgres (Benchmark Index)...");
        try {
            BenchmarkIndexProcessor processor = new BenchmarkIndexProcessor();
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
