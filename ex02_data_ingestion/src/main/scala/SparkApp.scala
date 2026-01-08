import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp

object SparkApp extends App {

  // ==============================================================================
  // 0) Config Spark + MinIO
  // ==============================================================================
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
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  val bucketName = "nyctaxiproject"
  val filesPath  = s"s3a://$bucketName/nyc_raw/"
  val zonesPath  = s"s3a://$bucketName/taxi_zone_lookup.csv"

  // ==============================================================================
  // 1) Lookup des zones (Essentiel pour la Service Zone)
  // ==============================================================================
  val zonesRaw = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(zonesPath)

  val puZones = zonesRaw
    .withColumnRenamed("LocationID",   "PULocationID")
    .withColumnRenamed("Borough",      "PU_Borough")
    .withColumnRenamed("Zone",         "PU_Zone")
    .withColumnRenamed("service_zone", "PU_service_zone") // ex: "Yellow Zone", "Airports"

  val doZones = zonesRaw
    .withColumnRenamed("LocationID",   "DOLocationID")
    .withColumnRenamed("Borough",      "DO_Borough")
    .withColumnRenamed("Zone",         "DO_Zone")
    .withColumnRenamed("service_zone", "DO_service_zone")

  // ==============================================================================
  // 2) Lecture brute
  // ==============================================================================
  val rawDF: DataFrame = spark.read.parquet(filesPath)
  println(s"Nb de lignes brutes : ${rawDF.count()}")

  // ==============================================================================
  // 3) Règles de contrat (Nettoyage de base)
  // ==============================================================================
  val allowedVendors      = Seq(1, 2, 6, 7)
  val allowedRatecodes    = Seq(1, 2, 3, 4, 5, 6, 99)
  val allowedPaymentTypes = Seq(0, 1, 2, 3, 4, 5, 6)
  val payingTypes         = Seq(1, 2)

  val amountCols = Seq(
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "Airport_fee",
    "cbd_congestion_fee", "total_amount"
  )

  val dfFilled = rawDF.na.fill(0.0, amountCols)

  val contractDF = dfFilled
    .filter($"passenger_count".isNotNull)
    .filter($"passenger_count" >= 1 && $"passenger_count" <= 6)
    .filter(year($"tpep_pickup_datetime") >= 2020) // Filtre temporel global
    .filter($"VendorID".isin(allowedVendors: _*))
    .filter($"RatecodeID".isin(allowedRatecodes: _*))
    .filter($"payment_type".isin(allowedPaymentTypes: _*))
    .filter($"PULocationID".isNotNull && $"DOLocationID".isNotNull)
    .filter($"tpep_pickup_datetime".isNotNull && $"tpep_dropoff_datetime".isNotNull && $"tpep_dropoff_datetime" >= $"tpep_pickup_datetime")
    .filter($"trip_distance" > 0.0)
    .filter($"store_and_fwd_flag".isNull || $"store_and_fwd_flag".isin("Y", "N"))
    .filter($"fare_amount" >= 0.0)
    .filter($"total_amount" >= 0.0)
    .filter(!$"payment_type".isin(payingTypes: _*) || $"total_amount" > 0.0)

  // ==============================================================================
  // 4) Vitesse & Durée
  // ==============================================================================
  val withDuration = contractDF.withColumn(
    "trip_duration_hours",
    (unix_timestamp($"tpep_dropoff_datetime") - unix_timestamp($"tpep_pickup_datetime")) / 3600.0
  )

  val withSpeed = withDuration.withColumn(
    "avg_speed_mph",
    when($"trip_duration_hours" > 0.0, $"trip_distance" / $"trip_duration_hours").otherwise(lit(null))
  )

  val minDurationHours = 2.0 / 60.0   // 2 minutes minimum
  val maxSpeedMphHard  = 120.0        // Plafond physique absolu (avant filtre fin)

  val speedFilteredDF = withSpeed
    .filter($"avg_speed_mph".isNotNull)
    .filter($"trip_duration_hours" >= minDurationHours)
    .filter($"avg_speed_mph" <= maxSpeedMphHard)

  // ==============================================================================
  // 5) Tip ratio
  // ==============================================================================
  val baseDF = speedFilteredDF.withColumn(
    "tip_ratio",
    when($"total_amount" > 0.0, $"tip_amount" / $"total_amount").otherwise(lit(null))
  )

  // ==============================================================================
  // 6) Calcul des bornes IQR (SMART: Sur Standard Rate uniquement)
  // ==============================================================================
  // On calcule la "normalité" statistique uniquement sur les trajets standards
  val standardTripsDF = baseDF.filter($"RateCodeID" === 1)

  val iqrCols = Seq("trip_distance", "fare_amount", "total_amount", "tip_amount", "avg_speed_mph", "trip_duration_hours", "tip_ratio")
  val quantileProbs = Array(0.25, 0.75)
  val relError = 0.01

  val iqrBounds: Map[String, (Double, Double)] = iqrCols.map { colName =>
    val Array(q1, q3) = standardTripsDF.stat.approxQuantile(colName, quantileProbs, relError)
    val iqr = q3 - q1
    val lower = q1 - 1.5 * iqr
    val upper = q3 + 1.5 * iqr
    colName -> (lower, upper)
  }.toMap

  // ==============================================================================
  // 7) Enrichissement Zones (CRITIQUE pour Service Zone)
  // ==============================================================================
  val withZones = baseDF
    .join(puZones, Seq("PULocationID"), "left")
    .join(doZones, Seq("DOLocationID"), "left")

  // ==============================================================================
  // 8) Définition des règles d'outliers (VERSION RAFFINÉE "PRICE PER MILE")
  // ==============================================================================

  // Seuils métier
  val tinyDistanceThreshold = 0.05       // < 0.05 mile = louche si cher
  val maxPricePerMile = 25.0             // 25$/mile c'est énorme (taxi normal ~3-6$/mile)
  val maxPriceForTinyDist = 20.0         // Si distance < 0.05, payer > 20$ est une erreur

  // Seuils Vitesse
  val speedYellowZoneThreshold = 45.0
  val speedGlobalSoftThreshold = 80.0

  // Récupération des bornes IQR (On les garde pour info, mais on ne filtrera plus "bêtement" dessus)
  val (distLow, distHigh) = iqrBounds("trip_distance")
  val (fareLow, fareHigh) = iqrBounds("fare_amount")
  val (totalLow, totalHigh) = iqrBounds("total_amount")
  val (tipAmtLow, tipAmtHigh) = iqrBounds("tip_amount")
  val (speedLow, speedHigh) = iqrBounds("avg_speed_mph")
  val (durLow, durHigh) = iqrBounds("trip_duration_hours")
  val (tipRatioLow, tipRatioHigh) = iqrBounds("tip_ratio")

  val withOutlierFlags = withZones
    // Contexte
    .withColumn("is_airport_trip", $"PU_service_zone" === "Airports" || $"DO_service_zone" === "Airports")
    .withColumn("is_yellow_zone_trip", $"PU_service_zone" === "Yellow Zone" && $"DO_service_zone" === "Yellow Zone")

    // --- NOUVELLE MÉTRIQUE CLÉ ---
    .withColumn("price_per_mile", when($"trip_distance" > 0.1, $"total_amount" / $"trip_distance").otherwise(lit(0.0)))

    // Flags IQR (Statistiques pures)
    .withColumn("out_trip_distance_iqr", $"trip_distance" < distLow || $"trip_distance" > distHigh)
    .withColumn("out_total_amount_iqr", $"total_amount" < totalLow || $"total_amount" > totalHigh)
    .withColumn("out_avg_speed_iqr", $"avg_speed_mph" < speedLow || $"avg_speed_mph" > speedHigh)
    .withColumn("out_duration_iqr", $"trip_duration_hours" < durLow || $"trip_duration_hours" > durHigh)

    // --- RÈGLES MÉTIER (Celles qui comptent vraiment) ---

    // 1. L'Horreur de la ligne 4 : Prix au mile aberrant
    // (ex: 237$ / 0.84 mile = 282$/mile -> FLAG !)
    .withColumn("out_price_per_mile_aberrant",
      ($"trip_distance" > 0.5) && ($"price_per_mile" > maxPricePerMile) && (!$"is_airport_trip")
    )

    // 2. L'Horreur statique : Distance nulle mais prix élevé
    .withColumn("out_expensive_tiny_trip",
      ($"trip_distance" < tinyDistanceThreshold) && ($"total_amount" > maxPriceForTinyDist)
    )

    // 3. Vitesse excessive (Classique)
    .withColumn("out_speed_excessive",
      ($"is_yellow_zone_trip" && $"avg_speed_mph" > speedYellowZoneThreshold) ||
        (!$"is_yellow_zone_trip" && $"avg_speed_mph" > speedGlobalSoftThreshold)
    )

    // 4. Incohérences
    .withColumn("out_duration_too_long", $"trip_duration_hours" > 5.0) // > 5h
    .withColumn("out_negative_values", $"total_amount" < 0.0 || $"fare_amount" < 0.0)

    // --- DÉCISION FINALE ---
    .withColumn("is_outlier",
      // On flag si une règle MÉTIER est cassée (ce sont les vraies horreurs)
      $"out_price_per_mile_aberrant" ||
        $"out_expensive_tiny_trip" ||
        $"out_speed_excessive" ||
        $"out_duration_too_long" ||
        $"out_negative_values" ||

        // Pour les stats IQR (Distance/Prix), on est BEAUCOUP plus souple.
        // On ne considère un "Long Trip" (IQR) comme outlier QUE si le prix n'est pas cohérent.
        // Si le prix est élevé MAIS que la distance est élevée aussi, on garde (c'est ta ligne 0).
        (
          ($"out_total_amount_iqr" && $"out_trip_distance_iqr") && // Si c'est Loin ET Cher...
            ($"price_per_mile" > 20.0) // ...on ne flag que si c'est TROP cher au mile.
          )
    )

  // ==============================================================================
  // 9) Préparation DWH (Mise à jour des raisons)
  // ==============================================================================

  def reason(colName: String, reasonTag: String) = when(col(colName), lit(reasonTag)).otherwise(lit(null))

  val dwhReadyDF = withOutlierFlags
    .withColumn("date_id", date_format($"tpep_pickup_datetime", "yyyyMMdd").cast("int"))
    .withColumn("trip_duration_minutes", $"trip_duration_hours" * 60.0)

    .withColumn("outlier_reasons_array", array(
      reason("out_speed_excessive", "High Speed"),
      reason("out_price_per_mile_aberrant", "Price/Mile > $25"), // <--- La raison qui va s'afficher pour la ligne 4
      reason("out_expensive_tiny_trip", "Expensive Tiny Trip"),
      reason("out_duration_too_long", "Duration > 5h"),
      reason("out_negative_values", "Negative Amount")
    ))
    .withColumn("outlier_reason", concat_ws(", ", expr("filter(outlier_reasons_array, x -> x is not null)")))

    .select(

      $"date_id", $"VendorID".as("vendor_id"), $"RatecodeID".as("ratecode_id"),
      $"payment_type".as("payment_type_id"), $"PULocationID".as("pickup_location_id"),
      $"DOLocationID".as("dropoff_location_id"),
      $"PU_service_zone", $"DO_service_zone",
      $"tpep_pickup_datetime", $"tpep_dropoff_datetime",
      $"passenger_count", $"store_and_fwd_flag", $"trip_distance", $"fare_amount",
      $"extra", $"mta_tax", $"tip_amount", $"tolls_amount", $"improvement_surcharge",
      $"congestion_surcharge", $"Airport_fee".as("airport_fee"), $"cbd_congestion_fee",
      $"total_amount", $"trip_duration_minutes", $"avg_speed_mph", $"tip_ratio",
      $"is_outlier",
      $"outlier_reason"
    )



  dwhReadyDF.write.mode("overwrite").parquet(s"s3a://$bucketName/nyc_dwh_ready/")

  // Exports spécifiques ML
  val cleanForML = withOutlierFlags.filter(!$"is_outlier")
  val outliersOnly = withOutlierFlags.filter($"is_outlier")

  cleanForML.drop("out_trip_distance_iqr", "out_fare_amount_iqr").write.mode("overwrite").parquet(s"s3a://$bucketName/nyc_clean_for_ml/")
  outliersOnly.write.mode("overwrite").parquet(s"s3a://$bucketName/nyc_outliers_debug/")

  println("Pipeline terminé avec succès. Prise en compte des Service Zones effectuée.")
  spark.stop()
}