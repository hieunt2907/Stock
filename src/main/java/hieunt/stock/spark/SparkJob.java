package hieunt.stock.spark;

import hieunt.stock.spark.processor.MinioToPostgresProcessor;

public class SparkJob {
    
    public static void main(String[] args) {
        System.out.println("Starting Spark Job: MinIO to Postgres...");
        
        try {
            MinioToPostgresProcessor processor = new MinioToPostgresProcessor();
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

