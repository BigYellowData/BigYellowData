import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp

case class Trip(
                 VendorID: Option[Int],
                 tpep_pickup_datetime: Option[Timestamp],
                 tpep_dropoff_datetime: Option[Timestamp],
                 passenger_count: Option[Long],
                 trip_distance: Option[Double],
                 RatecodeID: Option[Long],
                 store_and_fwd_flag: Option[String],
                 PULocationID: Option[Int],
                 DOLocationID: Option[Int],
                 payment_type: Option[Long],
                 fare_amount: Option[Double],
                 extra: Option[Double],
                 mta_tax: Option[Double],
                 tip_amount: Option[Double],
                 tolls_amount: Option[Double],
                 improvement_surcharge: Option[Double],
                 total_amount: Option[Double],
                 congestion_surcharge: Option[Double],
                 Airport_fee: Option[Double],
                 cbd_congestion_fee: Option[Double]
               )

object SparkApp extends App {

  // ========= 0) Config Spark + MinIO =========

  val user     = sys.env.getOrElse("MINIO_ROOT_USER", "minioadmin")
  val password = sys.env.getOrElse("MINIO_ROOT_PASSWORD", "minioadmin")
  val endpoint = sys.env.getOrElse("MINIO_ENDPOINT", "http://minio:9000")

  val spark = SparkSession.builder()
    .appName("SparkApp")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.access.key", user)
    .config("spark.hadoop.fs.s3a.secret.key", password)
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  val bucketName = "nyctaxiproject"
  val filesPath  = s"s3a://$bucketName/nyc_raw/"
  val zonesPath  = s"s3a://$bucketName/taxi_zone_lookup.csv"

  // ========= 1) Lookup des zones =========

  val zonesRaw = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(zonesPath)

  val puZones = zonesRaw
    .withColumnRenamed("LocationID",   "PULocationID")
    .withColumnRenamed("Borough",      "PU_Borough")
    .withColumnRenamed("Zone",         "PU_Zone")
    .withColumnRenamed("service_zone", "PU_service_zone")

  val doZones = zonesRaw
    .withColumnRenamed("LocationID",   "DOLocationID")
    .withColumnRenamed("Borough",      "DO_Borough")
    .withColumnRenamed("Zone",         "DO_Zone")
    .withColumnRenamed("service_zone", "DO_service_zone")

  // ========= 2) Lecture brute =========

  val rawDF: DataFrame = spark.read.parquet(filesPath)
  println(s"Nb de lignes brutes : ${rawDF.count()}")

  // ========= 3) Règles de contrat (logique métier "dures") =========

  val allowedVendors      = Seq(1, 2, 6, 7)
  val allowedRatecodes    = Seq(1, 2, 3, 4, 5, 6, 99)
  val allowedPaymentTypes = Seq(0, 1, 2, 3, 4, 5, 6)
  val payingTypes         = Seq(1, 2)

  val amountCols = Seq(
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
    "total_amount"
  )

  val dfFilled = rawDF.na.fill(0.0, amountCols)

  val contractDF =
    dfFilled
      .filter($"passenger_count".isNotNull)
      .filter($"passenger_count" >= 1 && $"passenger_count" <= 6)
      .filter($"VendorID".isin(allowedVendors: _*))
      .filter($"RatecodeID".isin(allowedRatecodes: _*))
      .filter($"payment_type".isin(allowedPaymentTypes: _*))
      .filter($"PULocationID".isNotNull && $"DOLocationID".isNotNull)
      .filter(
        $"tpep_pickup_datetime".isNotNull &&
          $"tpep_dropoff_datetime".isNotNull &&
          $"tpep_dropoff_datetime" >= $"tpep_pickup_datetime"
      )
      .filter($"trip_distance" > 0.0)
      .filter(
        $"store_and_fwd_flag".isNull ||
          $"store_and_fwd_flag".isin("Y", "N")
      )
      .filter($"fare_amount"           >= 0.0)
      .filter($"extra"                 >= 0.0)
      .filter($"mta_tax"               >= 0.0)
      .filter($"tip_amount"            >= 0.0)
      .filter($"tolls_amount"          >= 0.0)
      .filter($"improvement_surcharge" >= 0.0)
      .filter($"congestion_surcharge"  >= 0.0)
      .filter($"Airport_fee"           >= 0.0)
      .filter($"cbd_congestion_fee"    >= 0.0)
      .filter($"total_amount"          >= 0.0)
      .filter(
        !$"payment_type".isin(payingTypes: _*) ||
          $"total_amount" > 0.0
      )

  println(s"Après règles de contrat : ${contractDF.count()}")

  // ========= 4) Vitesse (durée + avg_speed) =========

  val withDuration =
    contractDF.withColumn(
      "trip_duration_hours",
      (unix_timestamp($"tpep_dropoff_datetime") -
        unix_timestamp($"tpep_pickup_datetime")) / 3600.0
    )

  val withSpeed =
    withDuration.withColumn(
      "avg_speed_mph",
      when($"trip_duration_hours" > 0.0,
        $"trip_distance" / $"trip_duration_hours"
      ).otherwise(lit(null))
    )

  // Règles "dures" réalistes
  val minDurationHours = 2.0 / 60.0   // >= 2 minutes
  val maxSpeedMphHard  = 72.0         // plafond "hard" global

  val speedFilteredDF =
    withSpeed
      .filter($"avg_speed_mph".isNotNull)
      .filter($"trip_duration_hours" >= minDurationHours)
      .filter($"avg_speed_mph" <= maxSpeedMphHard)

  println(s"Après filtre vitesse (hard) : ${speedFilteredDF.count()}")

  // ========= 5) Tip ratio de base (avant join zones) =========

  val baseDF =
    speedFilteredDF.withColumn(
      "tip_ratio",
      when($"total_amount" > 0.0, $"tip_amount" / $"total_amount")
        .otherwise(lit(null))
    )

  // ========= 6) Calcul des bornes IQR (boxplot) pour les colonnes clés =========

  val iqrCols = Seq(
    "trip_distance",
    "fare_amount",
    "total_amount",
    "tip_amount",
    "avg_speed_mph",
    "trip_duration_hours",
    "tip_ratio"
  )

  val quantileProbs = Array(0.25, 0.75) // Q1, Q3
  val relError      = 0.01

  val iqrBounds: Map[String, (Double, Double)] =
    iqrCols.map { colName =>
      val Array(q1, q3) = baseDF.stat.approxQuantile(colName, quantileProbs, relError)
      val iqr           = q3 - q1
      val lower         = q1 - 1.5 * iqr
      val upper         = q3 + 1.5 * iqr
      colName -> (lower, upper)
    }.toMap

  // ========= 7) Enrichissement avec les zones PU/DO =========

  val withZones =
    baseDF
      .join(puZones, Seq("PULocationID"), "left")
      .join(doZones, Seq("DOLocationID"), "left")

  // ========= 8) Définition des règles d'outliers =========

  // Seuils métier (faciles à ajuster)
  val tinyDistanceThreshold     = 0.02   // ~30 mètres
  val veryLongDistanceThreshold = 50.0   // 50 miles, très rare à NYC
  val speedGlobalSoftThreshold  = 70.0   // au-dessus = très rapide
  val speedManhattanThreshold   = 60.0   // Manhattan->Manhattan très suspect au-dessus
  val longTripHoursThreshold    = 6.0    // > 6h = très étrange
  val highTipRatioThreshold     = 0.8    // > 80% du total en tip = très atypique

  val (distLow, distHigh)         = iqrBounds("trip_distance")
  val (fareLow, fareHigh)         = iqrBounds("fare_amount")
  val (totalLow, totalHigh)       = iqrBounds("total_amount")
  val (tipAmtLow, tipAmtHigh)     = iqrBounds("tip_amount")
  val (speedLow, speedHigh)       = iqrBounds("avg_speed_mph")
  val (durLow, durHigh)           = iqrBounds("trip_duration_hours")
  val (tipRatioLow, tipRatioHigh) = iqrBounds("tip_ratio")

  val withOutlierFlags =
    withZones
      // IQR / boxplot-based
      .withColumn(
        "out_trip_distance_iqr",
        $"trip_distance" < distLow || $"trip_distance" > distHigh
      )
      .withColumn(
        "out_fare_amount_iqr",
        $"fare_amount" < fareLow || $"fare_amount" > fareHigh
      )
      .withColumn(
        "out_total_amount_iqr",
        $"total_amount" < totalLow || $"total_amount" > totalHigh
      )
      .withColumn(
        "out_tip_amount_iqr",
        $"tip_amount" < tipAmtLow || $"tip_amount" > tipAmtHigh
      )
      .withColumn(
        "out_avg_speed_iqr",
        $"avg_speed_mph" < speedLow || $"avg_speed_mph" > speedHigh
      )
      .withColumn(
        "out_duration_iqr",
        $"trip_duration_hours" < durLow || $"trip_duration_hours" > durHigh
      )
      .withColumn(
        "out_tip_ratio_iqr",
        $"tip_ratio".isNotNull &&
          ($"tip_ratio" < tipRatioLow || $"tip_ratio" > tipRatioHigh)
      )

      // Règles métier supplémentaires
      .withColumn("out_tiny_distance",        $"trip_distance" < tinyDistanceThreshold)
      .withColumn("out_very_long_distance",   $"trip_distance" > veryLongDistanceThreshold)
      .withColumn("out_speed_global_soft",    $"avg_speed_mph" > speedGlobalSoftThreshold)
      .withColumn(
        "out_speed_manhattan",
        $"PU_Borough" === "Manhattan" &&
          $"DO_Borough" === "Manhattan" &&
          $"avg_speed_mph" > speedManhattanThreshold
      )
      .withColumn("out_long_duration",        $"trip_duration_hours" > longTripHoursThreshold)
      .withColumn("out_tip_ratio_high",       $"tip_ratio" > highTipRatioThreshold)
      .withColumn("out_tip_ratio_negative",   $"tip_ratio" < 0.0)
      .withColumn(
        "out_fare_zero_tip_positive",
        $"fare_amount" === 0.0 && $"tip_amount" > 0.0
      )


      // Flag global : un trajet est outlier si AU MOINS une règle déclenche
      .withColumn(
        "is_outlier",
        $"out_trip_distance_iqr"      ||
          $"out_fare_amount_iqr"        ||
          $"out_total_amount_iqr"       ||
          $"out_tip_amount_iqr"         ||
          $"out_avg_speed_iqr"          ||
          $"out_duration_iqr"           ||
          $"out_tip_ratio_iqr"          ||
          $"out_tiny_distance"          ||
          $"out_very_long_distance"     ||
          $"out_speed_global_soft"      ||
          $"out_speed_manhattan"        ||
          $"out_long_duration"          ||
          $"out_tip_ratio_high"         ||
          $"out_tip_ratio_negative"     ||
          $"out_fare_zero_tip_positive"
      )

  // ========= 9) Séparation clean / outliers =========

  val outliersDF =
    withOutlierFlags.filter($"is_outlier")

  val cleanForML =
    withOutlierFlags.filter(!$"is_outlier")

  println(s"Nb trajets après règles métier : ${withZones.count()}")
  println(s"Nb outliers détectés : ${outliersDF.count()}")
  println(s"Nb trajets 'clean' pour ML: ${cleanForML.count()}")

  // ========= 10) Exemple d'affichage d'outliers (pour debug / analyse) =========

  println("===== Exemples d'outliers détectés (top 50 par vitesse) =====")
  outliersDF
    .select(
      $"tpep_pickup_datetime",
      $"tpep_dropoff_datetime",
      $"trip_distance",
      $"trip_duration_hours",
      $"avg_speed_mph",
      $"fare_amount",
      $"tip_amount",
      $"total_amount",
      $"tip_ratio",
      $"passenger_count",
      $"PU_Borough",
      $"PU_Zone",
      $"DO_Borough",
      $"DO_Zone",
      $"out_trip_distance_iqr",
      $"out_fare_amount_iqr",
      $"out_total_amount_iqr",
      $"out_tip_amount_iqr",
      $"out_avg_speed_iqr",
      $"out_duration_iqr",
      $"out_tip_ratio_iqr",
      $"out_tiny_distance",
      $"out_very_long_distance",
      $"out_speed_global_soft",
      $"out_speed_manhattan",
      $"out_long_duration",
      $"out_tip_ratio_high",
      $"out_tip_ratio_negative",
      $"out_fare_zero_tip_positive"

    )
    .orderBy(desc("avg_speed_mph"))
    .show(50, truncate = false)

  // ========= 11) Dataset[Trip] "clean" pour usage ML =========

  val cleanTripDS = {
    cleanForML
      .select(
        $"VendorID",
        $"tpep_pickup_datetime",
        $"tpep_dropoff_datetime",
        $"passenger_count",
        $"trip_distance",
        $"RatecodeID",
        $"store_and_fwd_flag",
        $"PULocationID",
        $"DOLocationID",
        $"payment_type",
        $"fare_amount",
        $"extra",
        $"mta_tax",
        $"tip_amount",
        $"tolls_amount",
        $"improvement_surcharge",
        $"total_amount",
        $"congestion_surcharge",
        $"Airport_fee",
        $"cbd_congestion_fee"
      )
      .as[Trip]

    println(s"Lignes : ${cleanForML.count()}")
  }

  // ========= 12) Option : sauvegarde dans MinIO =========

  cleanForML.toDF
    .write
    .mode("overwrite")
    .parquet(s"s3a://$bucketName/nyc_clean_for_ml/")

  outliersDF
    .write
    .mode("overwrite")
    .parquet(s"s3a://$bucketName/nyc_outliers_full/")


  spark.stop()
}
