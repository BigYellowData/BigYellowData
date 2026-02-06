import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Logger, Level}

/**
 * Exercise 3: Data Loading Pipeline (ETL - Load Phase).
 *
 * This application is responsible for loading the refined data (cleaned and enriched)
 * from the Data Lake (MinIO) into the Data Warehouse (PostgreSQL).
 * It maps the Spark DataFrame to the target 'fact_trip' table schema and performs
 * an optimized JDBC bulk insert.
 *
 * @author BigYellowData Team
 * @version 1.0
 */
object SparkApp extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  // ==============================================================================
  // CONFIGURATION
  // ==============================================================================

  // MinIO config
  val user = sys.env.getOrElse("MINIO_ROOT_USER", "minioadmin")
  val password = sys.env.getOrElse("MINIO_ROOT_PASSWORD", "minioadmin")
  val endpoint = sys.env.getOrElse("MINIO_ENDPOINT", "http://minio:9000")
  val bucketName = "nyctaxiproject"
  val inputPath = s"s3a://$bucketName/dwh/yellow_taxi_refined/"

  // PostgreSQL config
  val jdbcUrl = "jdbc:postgresql://postgres-dw:5432/nyc_data_warehouse"
  val jdbcUser = "user_dw"
  val jdbcPassword = "password_dw"
  val jdbcTable = "dw.fact_trip"

  println("=" * 70)
  println("  Ex03: Loading fact_trip into PostgreSQL")
  println("=" * 70)

  val startTime = System.currentTimeMillis()

  // ==============================================================================
  // SPARK SESSION
  // ==============================================================================
  val spark = SparkSession.builder()
    .appName("Ex03-LoadFactTrip")
    .master("spark://spark-master:7077")
    // S3/MinIO
    .config("spark.hadoop.fs.s3a.access.key", user)
    .config("spark.hadoop.fs.s3a.secret.key", password)
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    // Optimisations
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  // ==============================================================================
  // STEP 1: Read Refined Data
  // ==============================================================================s
  println("\n[1/3] Reading refined data from MinIO...")

  val refinedDF = spark.read.parquet(inputPath)

  val rowCount = refinedDF.count()
  println(s"      -> $rowCount rows loaded from parquet")

  // ==============================================================================
  // STEP 2: Schema Mapping
  // ==============================================================================
  println("\n[2/3] Preparing data for PostgreSQL...")

  // Select columns matching fact_trip schema (excluding trip_id which is auto-generated)
  val factTripDF = refinedDF.select(
    $"date_id",
    $"vendor_id",
    $"ratecode_id",
    $"payment_type_id",
    $"pickup_location_id",
    $"dropoff_location_id",
    $"tpep_pickup_datetime",
    $"tpep_dropoff_datetime",
    $"passenger_count",
    $"store_and_fwd_flag",
    $"trip_distance",
    $"fare_amount",
    $"extra",
    $"mta_tax",
    $"tip_amount",
    $"tolls_amount",
    $"improvement_surcharge",
    $"congestion_surcharge",
    $"airport_fee",
    $"cbd_congestion_fee",
    $"total_amount",
    $"trip_duration_minutes",
    $"avg_speed_mph",
    $"tip_ratio",
    $"is_outlier",
    $"outlier_reason"
  )

  // ==============================================================================
  // STEP 3: Write to JDBC (Bulk Insert)
  // ==============================================================================
  println("\n[3/3] Writing to PostgreSQL (dw.fact_trip)...")
  println("      This may take several minutes for large datasets...")

  val jdbcProps = new java.util.Properties()
  jdbcProps.setProperty("user", jdbcUser)
  jdbcProps.setProperty("password", jdbcPassword)
  jdbcProps.setProperty("driver", "org.postgresql.Driver")
  // Batch insert for better performance
  jdbcProps.setProperty("batchsize", "10000")
  jdbcProps.setProperty("rewriteBatchedStatements", "true")

  factTripDF
    .repartition(8) // Parallelize writes
    .write
    .mode(SaveMode.Append)
    .jdbc(jdbcUrl, jdbcTable, jdbcProps)

  val duration = (System.currentTimeMillis() - startTime) / 1000.0

  println("\n" + "=" * 70)
  println(f"  SUCCESS: $rowCount%,d rows inserted into $jdbcTable")
  println(f"  Duration: $duration%.1f seconds")
  println("=" * 70)

  spark.stop()
}
