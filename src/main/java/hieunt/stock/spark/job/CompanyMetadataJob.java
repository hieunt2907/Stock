package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.CompanyMetadataProcessor;

public class CompanyMetadataJob {
    public static void main(String[] args) {
        System.out.println("Starting Spark Job: MinIO to Postgres (Company Metadata)...");
        try {
            CompanyMetadataProcessor processor = new CompanyMetadataProcessor();
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
