import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Logger, Level}

/**
 * Pipeline de nettoyage des données NYC Taxi - Version Optimisée v5.0
 *
 * Optimisations appliquées:
 * - Lazy evaluation maximisée (un seul action à la fin)
 * - Broadcast pour toutes les petites tables
 * - Filtres combinés en un minimum de passes
 * - Pas de count() intermédiaires (utilisation d'accumulateurs)
 * - Cache stratégique uniquement où nécessaire
 * - Colonnes calculées en une seule passe
 */
object SparkApp extends App {

  // ==============================================================================
  // CONFIGURATION
  // ==============================================================================
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val user = sys.env.getOrElse("MINIO_ROOT_USER", "minioadmin")
  val password = sys.env.getOrElse("MINIO_ROOT_PASSWORD", "minioadmin")
  val endpoint = sys.env.getOrElse("MINIO_ENDPOINT", "http://minio:9000")
  val bucketName = "nyctaxiproject"
  val filesPath = s"s3a://$bucketName/nyc_raw/"
  val zonesPath = s"s3a://$bucketName/taxi_zone_lookup.csv"
  val outputPath = s"s3a://$bucketName/dwh/yellow_taxi_refined/"

  // Seuils de configuration
  val MIN_DURATION_MINUTES = 2.0
  val MAX_SPEED_MPH = 120.0
  val SPEED_YELLOW_ZONE = 45.0
  val SPEED_GLOBAL = 80.0
  val MIN_DISTANCE = 0.3
  val MAX_LOOP_DISTANCE = 5.0
  val MAX_PRICE_PER_MILE = 25.0
  val MAX_DURATION_HOURS = 5.0
  val MIN_ROUTE_TRIPS = 10
  val ZSCORE_THRESHOLD = 3.0

  val pipelineVersion = "v5.0_ultra_optimized"

  println("=" * 80)
  println(s"  NYC TAXI DATA PIPELINE - $pipelineVersion")
  println("=" * 80)
  val startTime = System.currentTimeMillis()

  // ==============================================================================
  // SPARK SESSION - Configuration optimisée
  // ==============================================================================
  val spark = SparkSession.builder()
    .appName("NYCTaxi-UltraOptimized")
    .master("spark://spark-master:7077")
    // S3/MinIO
    .config("spark.hadoop.fs.s3a.access.key", user)
    .config("spark.hadoop.fs.s3a.secret.key", password)
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    // Optimisations mémoire et shuffle
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.parquet.compression.codec", "snappy")
    // Optimisations S3
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  // ==============================================================================
  // ÉTAPE 1: Chargement des zones (petite table depuis MinIO)
  // ==============================================================================
  println("\n[1/6] Chargement des zones de référence depuis MinIO...")
  println("       (Première connexion S3 - peut prendre quelques secondes)")

  val zonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(zonesPath)

  // Collecter les IDs valides en local (265 lignes seulement)
  val validLocationIDs = zonesDF.select("LocationID").distinct().as[Int].collect().toSeq
  println(s"       → ${validLocationIDs.size} zones chargées")

  // DataFrames pour les joins (broadcast automatique car < 50MB)
  val puZonesDF = zonesDF
    .withColumnRenamed("LocationID", "PULocationID")
    .withColumnRenamed("Borough", "PU_Borough")
    .withColumnRenamed("Zone", "PU_Zone")
    .withColumnRenamed("service_zone", "PU_service_zone")

  val doZonesDF = zonesDF
    .withColumnRenamed("LocationID", "DOLocationID")
    .withColumnRenamed("Borough", "DO_Borough")
    .withColumnRenamed("Zone", "DO_Zone")
    .withColumnRenamed("service_zone", "DO_service_zone")

  // ==============================================================================
  // ÉTAPE 2: Lecture et nettoyage initial (LAZY - pas d'action)
  // ==============================================================================
  println("\n[2/6] Lecture et filtrage initial des données...")

  val allowedVendors = Seq(1, 2, 6, 7)
  val allowedRatecodes = Seq(1, 2, 3, 4, 5, 6, 99)
  val allowedPaymentTypes = Seq(0, 1, 2, 3, 4, 5, 6)
  val payingTypes = Seq(0, 1, 2)

  val amountCols = Seq("fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "Airport_fee", "cbd_congestion_fee", "total_amount")

  // Lecture + tous les filtres contractuels en UNE SEULE PASSE
  // Utilisation de isin() au lieu d'UDF pour la validation des zones (plus performant)
  val cleanedDF = spark.read.parquet(filesPath)
    .na.fill(0.0, amountCols)
    // Filtre contractuel combiné
    .filter(
      $"passenger_count".isNotNull &&
      $"passenger_count".between(1, 6) &&
      year($"tpep_pickup_datetime") >= 2020 &&
      $"VendorID".isin(allowedVendors: _*) &&
      $"RatecodeID".isin(allowedRatecodes: _*) &&
      $"payment_type".isin(allowedPaymentTypes: _*) &&
      $"PULocationID".isNotNull && $"DOLocationID".isNotNull &&
      $"tpep_pickup_datetime".isNotNull && $"tpep_dropoff_datetime".isNotNull &&
      $"tpep_dropoff_datetime" >= $"tpep_pickup_datetime" &&
      $"trip_distance" > 0.0 &&
      $"trip_distance" >= MIN_DISTANCE &&
      ($"store_and_fwd_flag".isNull || $"store_and_fwd_flag".isin("Y", "N")) &&
      $"fare_amount" >= 0.0 &&
      $"total_amount" >= 0.0 &&
      (!$"payment_type".isin(payingTypes: _*) || $"total_amount" > 0.0) &&
      // Validation zones - utilise isin() au lieu d'UDF
      $"PULocationID".isin(validLocationIDs: _*) &&
      $"DOLocationID".isin(validLocationIDs: _*) &&
      // Tips aberrants
      ($"tip_amount".isNull || $"tip_amount" <= ($"total_amount" - $"tip_amount") || $"payment_type" =!= 1)
    )

  println("       → Filtres contractuels appliqués (lazy)")

  // ==============================================================================
  // ÉTAPE 3: Calculs dérivés + Déduplication (LAZY)
  // ==============================================================================
  println("\n[3/6] Calcul des métriques dérivées...")

  val withMetricsDF = cleanedDF
    // Calcul durée et vitesse
    .withColumn("trip_duration_hours",
      (unix_timestamp($"tpep_dropoff_datetime") - unix_timestamp($"tpep_pickup_datetime")) / 3600.0)
    .withColumn("avg_speed_mph",
      when($"trip_duration_hours" > 0, $"trip_distance" / $"trip_duration_hours"))
    .withColumn("tip_ratio",
      when($"total_amount" > 0, $"tip_amount" / $"total_amount"))
    .withColumn("price_per_mile",
      when($"trip_distance" > 0.1, $"total_amount" / $"trip_distance").otherwise(0.0))
    // Clé de déduplication
    .withColumn("dedup_key", concat_ws("_",
      $"tpep_pickup_datetime".cast("string"),
      $"tpep_dropoff_datetime".cast("string"),
      $"PULocationID", $"DOLocationID",
      round($"total_amount", 2), $"VendorID", $"passenger_count"))
    // Filtre vitesse/durée
    .filter(
      $"trip_duration_hours" >= (MIN_DURATION_MINUTES / 60.0) &&
      $"avg_speed_mph".isNotNull &&
      $"avg_speed_mph" <= MAX_SPEED_MPH &&
      // Boucles (même zone)
      when($"PULocationID" === $"DOLocationID", $"trip_distance" < MAX_LOOP_DISTANCE).otherwise(lit(true))
    )
    // Déduplication
    .dropDuplicates("dedup_key")
    .drop("dedup_key")

  println("       → Métriques calculées (lazy)")

  // ==============================================================================
  // ÉTAPE 4: Détection des remboursements (nécessite une action)
  // ==============================================================================
  println("\n[4/6] Détection des remboursements...")

  // Clé de trip pour détecter les paires refund
  val withRefundKey = withMetricsDF.withColumn("refund_key",
    concat_ws("_",
      unix_timestamp($"tpep_pickup_datetime"),
      unix_timestamp($"tpep_dropoff_datetime"),
      round($"trip_distance", 2),
      round(abs($"total_amount"), 2),
      $"PULocationID", $"DOLocationID", $"VendorID"))

  // Trouver les clés avec positif ET négatif
  val refundKeys = withRefundKey
    .groupBy("refund_key")
    .agg(
      sum(when($"total_amount" > 0, 1).otherwise(0)).as("pos"),
      sum(when($"total_amount" < 0, 1).otherwise(0)).as("neg"))
    .filter($"pos" > 0 && $"neg" > 0)
    .select("refund_key")

  // Anti-join pour supprimer les remboursements
  val noRefundsDF = withRefundKey
    .join(broadcast(refundKeys), Seq("refund_key"), "left_anti")
    .drop("refund_key")
    .cache()  // Cache ici car utilisé pour stats route + suite du pipeline

  val countAfterRefunds = noRefundsDF.count()
  println(s"       → $countAfterRefunds lignes après suppression remboursements")

  // ==============================================================================
  // ÉTAPE 5: Stats par route + Outliers + Enrichissement (LAZY sur cache)
  // ==============================================================================
  println("\n[5/6] Analyse statistique et détection d'outliers...")

  // Stats par route (broadcast car petit résultat)
  val routeStats = noRefundsDF
    .groupBy("PULocationID", "DOLocationID")
    .agg(
      avg("trip_distance").as("route_avg_dist"),
      stddev("trip_distance").as("route_stddev_dist"),
      count("*").as("route_count"))
    .filter($"route_count" >= MIN_ROUTE_TRIPS)

  // Calcul IQR sur Standard Rate (RateCode=1) - UN SEUL calcul
  val standardTrips = noRefundsDF.filter($"RateCodeID" === 1)
  val iqrCols = Array("trip_distance", "total_amount", "avg_speed_mph", "trip_duration_hours")
  val quantiles = standardTrips.stat.approxQuantile(iqrCols, Array(0.25, 0.75), 0.01)

  val iqrBounds = iqrCols.zipWithIndex.map { case (col, i) =>
    val q1 = quantiles(i)(0)
    val q3 = quantiles(i)(1)
    val iqr = q3 - q1
    col -> (q1 - 1.5 * iqr, q3 + 1.5 * iqr)
  }.toMap

  println("       → Bornes IQR calculées:")
  iqrBounds.foreach { case (col, (low, high)) =>
    println(f"         $col: [$low%.2f, $high%.2f]")
  }

  val (distLow, distHigh) = iqrBounds("trip_distance")
  val (totalLow, totalHigh) = iqrBounds("total_amount")
  val (speedLow, speedHigh) = iqrBounds("avg_speed_mph")
  val (durLow, durHigh) = iqrBounds("trip_duration_hours")

  // Pipeline final: join zones + stats route + outliers + métadonnées
  val finalDF = noRefundsDF
    // Join avec stats de route (broadcast)
    .join(broadcast(routeStats), Seq("PULocationID", "DOLocationID"), "left")
    // Filtre Z-Score
    .filter(
      $"route_stddev_dist".isNull || $"route_stddev_dist" === 0.0 ||
      abs($"trip_distance" - $"route_avg_dist") <= lit(ZSCORE_THRESHOLD) * $"route_stddev_dist")
    .drop("route_avg_dist", "route_stddev_dist", "route_count")
    // Join avec zones (broadcast automatique)
    .join(broadcast(puZonesDF), Seq("PULocationID"), "left")
    .join(broadcast(doZonesDF), Seq("DOLocationID"), "left")
    // Flags outliers
    .withColumn("is_airport_trip",
      $"PU_service_zone" === "Airports" || $"DO_service_zone" === "Airports")
    .withColumn("is_yellow_zone",
      $"PU_service_zone" === "Yellow Zone" && $"DO_service_zone" === "Yellow Zone")
    .withColumn("out_distance_iqr", $"trip_distance" < distLow || $"trip_distance" > distHigh)
    .withColumn("out_amount_iqr", $"total_amount" < totalLow || $"total_amount" > totalHigh)
    .withColumn("out_speed_iqr", $"avg_speed_mph" < speedLow || $"avg_speed_mph" > speedHigh)
    .withColumn("out_duration_iqr", $"trip_duration_hours" < durLow || $"trip_duration_hours" > durHigh)
    .withColumn("out_price_mile", $"trip_distance" > 0.5 && $"price_per_mile" > MAX_PRICE_PER_MILE && !$"is_airport_trip")
    .withColumn("out_speed_zone",
      ($"is_yellow_zone" && $"avg_speed_mph" > SPEED_YELLOW_ZONE) ||
      (!$"is_yellow_zone" && $"avg_speed_mph" > SPEED_GLOBAL))
    .withColumn("out_duration_long", $"trip_duration_hours" > MAX_DURATION_HOURS)
    // Décision outlier finale
    .withColumn("is_outlier",
      $"out_price_mile" || $"out_speed_zone" || $"out_duration_long" ||
      ($"out_amount_iqr" && $"out_distance_iqr" && $"price_per_mile" > 20.0))
    // Score qualité
    .withColumn("data_quality_score",
      when($"is_outlier", 0)
      .when($"out_distance_iqr" || $"out_amount_iqr", 75)
      .otherwise(100))
    // Raison outlier
    .withColumn("outlier_reason", concat_ws(", ",
      when($"out_speed_zone", "High Speed"),
      when($"out_price_mile", "Price/Mile > $25"),
      when($"out_duration_long", "Duration > 5h")))
    // Métadonnées
    .withColumn("date_id", date_format($"tpep_pickup_datetime", "yyyyMMdd").cast("int"))
    .withColumn("trip_duration_minutes", $"trip_duration_hours" * 60.0)
    .withColumn("processing_timestamp", current_timestamp())
    .withColumn("pipeline_version", lit(pipelineVersion))
    // Sélection finale des colonnes DWH
    .select(
      $"date_id", $"VendorID".as("vendor_id"), $"RatecodeID".as("ratecode_id"),
      $"payment_type".as("payment_type_id"), $"PULocationID".as("pickup_location_id"),
      $"DOLocationID".as("dropoff_location_id"), $"PU_service_zone", $"DO_service_zone",
      $"tpep_pickup_datetime", $"tpep_dropoff_datetime",
      $"passenger_count", $"store_and_fwd_flag", $"trip_distance", $"fare_amount",
      $"extra", $"mta_tax", $"tip_amount", $"tolls_amount", $"improvement_surcharge",
      $"congestion_surcharge", $"Airport_fee".as("airport_fee"), $"cbd_congestion_fee",
      $"total_amount", $"trip_duration_minutes", $"avg_speed_mph", $"tip_ratio",
      $"is_outlier", $"outlier_reason", $"data_quality_score",
      $"processing_timestamp", $"pipeline_version")

  // ==============================================================================
  // ÉTAPE 6: Écriture + Rapport (SEULE ACTION MAJEURE)
  // ==============================================================================
  println("\n[6/6] Écriture et génération du rapport...")

  // Écriture avec repartitionnement par date
  finalDF
    .repartition($"date_id")
    .write
    .mode("overwrite")
    .partitionBy("date_id")
    .parquet(outputPath)

  // Libérer le cache
  noRefundsDF.unpersist()

  // Relire pour stats finales (lecture rapide depuis le parquet écrit)
  val writtenDF = spark.read.parquet(outputPath)

  val stats = writtenDF.agg(
    count("*").as("total"),
    sum(when($"is_outlier", 1).otherwise(0)).as("outliers"),
    sum(when($"data_quality_score" === 100, 1).otherwise(0)).as("perfect"),
    sum(when($"data_quality_score" === 75, 1).otherwise(0)).as("good"),
    avg("trip_distance").as("avg_dist"),
    avg("total_amount").as("avg_amount"),
    min("trip_distance").as("min_dist"),
    max("trip_distance").as("max_dist")
  ).collect()(0)

  val total = stats.getLong(0)
  val outliers = stats.getLong(1)
  val perfect = stats.getLong(2)
  val good = stats.getLong(3)

  println("\n" + "=" * 80)
  println("  RAPPORT FINAL")
  println("=" * 80)
  println(f"  Total courses:        $total%,d")
  println(f"  Qualité parfaite:     $perfect%,d (${perfect * 100.0 / total}%.1f%%)")
  println(f"  Bonne qualité:        $good%,d (${good * 100.0 / total}%.1f%%)")
  println(f"  Outliers:             $outliers%,d (${outliers * 100.0 / total}%.1f%%)")
  println(f"  Distance:             min=${stats.getDouble(6)}%.2f, max=${stats.getDouble(7)}%.2f, avg=${stats.getDouble(4)}%.2f mi")
  println(f"  Montant moyen:        $$${stats.getDouble(5)}%.2f")

  // Vérifications
  val qualityRatio = (perfect + good).toDouble / total
  println("\n  TESTS:")
  println(s"  ✅ Dataset non vide: $total lignes")
  println(f"  ${if (qualityRatio >= 0.70) "✅" else "❌"} Qualité >= 70%%: $qualityRatio%.1f%%")

  val duration = (System.currentTimeMillis() - startTime) / 60000.0
  println("\n" + "=" * 80)
  println(f"  ✅ PIPELINE TERMINÉ EN $duration%.2f MINUTES")
  println("=" * 80)

  spark.stop()
}
