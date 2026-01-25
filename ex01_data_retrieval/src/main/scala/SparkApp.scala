import org.apache.spark.sql.{SparkSession, DataFrame}

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

  val localPath = "/data/raw/*.parquet"
  val outputPath = s"s3a://${bucketName}/nyc_raw/"

  val df : DataFrame = spark.read.parquet(localPath)
  df.write.mode("overwrite").parquet(outputPath)

  spark.stop()

}