package hieunt.stock.spark.job;

import hieunt.stock.spark.processor.CompanyMetadataProcessor;

public class CompanyMetadataJob {
    public static void main(String[] args) {
        String partitionPath = (args != null && args.length > 0) ? args[0] : null;
        System.out.println("Starting Spark Job: MinIO to Postgres (Company Metadata) | Partition: " + partitionPath);
        try {
            CompanyMetadataProcessor processor = new CompanyMetadataProcessor(partitionPath);
            processor.process();
            System.out.println("Job Finished Successfully!");
        } catch (Exception e) {
            System.err.println("Job Failed with error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
