package hieunt.stock.spark;

import hieunt.stock.config.SparkConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Spark {

    public static SparkSession getSparkSession() {
        SparkConf conf = new SparkConf()
                .setAppName(SparkConfig.APP_NAME)
                .setMaster(SparkConfig.MASTER)
                
                // CPU / Resource Config
                .set("spark.executor.memory", SparkConfig.EXECUTOR_MEMORY)
                .set("spark.executor.cores", SparkConfig.EXECUTOR_CORES)
                .set("spark.executor.instances", SparkConfig.EXECUTOR_INSTANCES)
                .set("spark.driver.memory", SparkConfig.DRIVER_MEMORY)
                .set("spark.driver.cores", SparkConfig.DRIVER_CORES)
                
                // Docker / Network Config
                // .set("spark.driver.host", "host.docker.internal")
                // .set("spark.driver.bindAddress", "0.0.0.0")
                
                // Memory Config
                .set("spark.sql.shuffle.partitions", SparkConfig.SQL_SHUFFLE_PARTITIONS)
                .set("spark.memory.fraction", SparkConfig.MEMORY_FRACTION)
                .set("spark.memory.storageFraction", SparkConfig.MEMORY_STORAGE_FRACTION)
                .set("spark.memory.offHeap.enabled",
                        String.valueOf(SparkConfig.MEMORY_OFF_HEAP_ENABLED))
                .set("spark.memory.offHeap.size", SparkConfig.MEMORY_OFF_HEAP_SIZE)

                // Gộp các file nhỏ thành partition 128MB để giảm số lượng Task
                .set("spark.sql.files.maxPartitionBytes", "134217728") 
                // Tăng chi phí "mở file" để Spark ưu tiên gộp file thay vì tạo task mới
                .set("spark.sql.files.openCostInBytes", "10485760")    
                // Đảm bảo số lượng partition tối thiểu khớp với cấu hình shuffle của bạn
                .set("spark.sql.files.minPartitionNum", SparkConfig.SQL_SHUFFLE_PARTITIONS)
                // -------------------------------------------------

                // Hadoop / MinIO S3A Config
                .set("spark.hadoop.fs.s3a.endpoint", hieunt.stock.config.MinIOConfig.ENDPOINT)
                .set("spark.hadoop.fs.s3a.access.key", hieunt.stock.config.MinIOConfig.ACCESS_KEY)
                .set("spark.hadoop.fs.s3a.secret.key", hieunt.stock.config.MinIOConfig.SECRET_KEY)
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.path.style.access",
                        String.valueOf(hieunt.stock.config.MinIOConfig.PATH_STYLE))
                .set("spark.hadoop.fs.s3a.connection.ssl.enabled",
                        String.valueOf(hieunt.stock.config.MinIOConfig.SECURE));

        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }
}