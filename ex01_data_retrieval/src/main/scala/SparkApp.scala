import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

object SparkApp extends App {

  val user = System.getenv("MINIO_ROOT_USER")
  val password = System.getenv("MINIO_ROOT_PASSWORD")
  val endpoint = System.getenv("MINIO_ENDPOINT")


  val spark = SparkSession.builder()
    .appName("SparkApp")
    .master("spark://spark-master:7077")
    .config("fs.s3a.access.key", user)
    .config("fs.s3a.secret.key", password)
    .config("fs.s3a.endpoint", endpoint)
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "6000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val bucketName="nyctaxiproject"

  // 1) Upload parquet files
  val localPath = "/data/raw/*.parquet"
  val outputPath = s"s3a://${bucketName}/nyc_raw/"

  val df : DataFrame = spark.read.parquet(localPath)
  df.write.mode("overwrite").parquet(outputPath)

  Predef.println("[INFO] ✅ Parquet files uploaded to MinIO")

  // 2) Upload taxi_zone_lookup.csv (as a single file, not directory)
  val csvLocalPath = "/data/raw/taxi_zone_lookup.csv"
  val csvOutputPath = s"s3a://${bucketName}/taxi_zone_lookup.csv"

  try {
    // Use Hadoop FileSystem API to copy file directly
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val srcPath = new Path(s"file://$csvLocalPath")
    val dstPath = new Path(csvOutputPath)

    // Get S3 FileSystem
    val s3Fs = FileSystem.get(dstPath.toUri, hadoopConf)
    val localFs = FileSystem.get(srcPath.toUri, hadoopConf)

    // Copy file from local to S3
    if (localFs.exists(srcPath)) {
      // Read local file and write to S3
      val inputStream = localFs.open(srcPath)
      val outputStream = s3Fs.create(dstPath, true)

      try {
        val buffer = new Array[Byte](4096)
        var bytesRead = inputStream.read(buffer)
        while (bytesRead > 0) {
          outputStream.write(buffer, 0, bytesRead)
          bytesRead = inputStream.read(buffer)
        }
        Predef.println("[INFO] ✅ taxi_zone_lookup.csv uploaded to MinIO")
      } finally {
        inputStream.close()
        outputStream.close()
      }
    } else {
      Predef.println(s"[WARN] ⚠️  File not found: $csvLocalPath")
      Predef.println("[WARN] This is not critical for Ex01, but Ex02 will need this file.")
    }
  } catch {
    case e: Exception =>
      Predef.println(s"[WARN] ⚠️  Could not upload taxi_zone_lookup.csv: ${e.getMessage}")
      Predef.println("[WARN] This is not critical for Ex01, but Ex02 will need this file.")
  }

  spark.stop()

}