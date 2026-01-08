import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp
import org.apache.log4j.{Logger, Level}

object SparkApp extends App {

  // ==============================================================================
  // 0) Configuration & Logger
  // ==============================================================================
  val println = Logger.getLogger(getClass.getName)

  // On coupe la racine "org" et spécifiquement "org.apache.spark"
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR) // Pour le S3/MinIO

  val user = sys.env.getOrElse("MINIO_ROOT_USER", "minioadmin")
  val password = sys.env.getOrElse("MINIO_ROOT_PASSWORD", "minioadmin")
  val endpoint = sys.env.getOrElse("MINIO_ENDPOINT", "http://minio:9000")
  val bucketName = "nyctaxiproject"
  val filesPath = s"s3a://$bucketName/nyc_raw/"
  val zonesPath = s"s3a://$bucketName/taxi_zone_lookup.csv"

  // Configuration des seuils (avec types corrects)
  val config: Map[String, Double] = Map(
    "MIN_DURATION_MINUTES" -> 2.0,
    "MAX_SPEED_MPH_HARD" -> 120.0,
    "SPEED_YELLOW_ZONE_THRESHOLD" -> 45.0,
    "SPEED_GLOBAL_SOFT_THRESHOLD" -> 80.0,
    "TINY_DISTANCE_THRESHOLD" -> 0.05,
    "MAX_PRICE_PER_MILE" -> 25.0,
    "MAX_PRICE_FOR_TINY_DIST" -> 20.0,
    "MAX_DURATION_HOURS" -> 5.0,
    "MIN_TRIP_COUNT_PER_ROUTE" -> 10.0,
    "DISTANCE_TOLERANCE_FACTOR" -> 0.5
  )

  val pipelineVersion = "v3.0_production_ready"

  logger.info(s"=== Pipeline $pipelineVersion démarré ===")
  val pipelineStartTime = System.currentTimeMillis()

  val spark = SparkSession.builder()
    .appName("SparkApp-ProductionReady")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.access.key", user)
    .config("spark.hadoop.fs.s3a.secret.key", password)
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  // ==============================================================================
  // 1) Lecture et validation des zones
  // ==============================================================================
  logger.info("Chargement du lookup des zones...")

  val zonesRaw = try {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(zonesPath)
  } catch {
    case e: Exception =>
      logger.error(s"Erreur critique lors de la lecture des zones: ${e.getMessage}")
      spark.stop()
      throw e
  }

  assert(zonesRaw.count() > 0, "Le fichier des zones est vide!")
  logger.info(s"Zones chargées: ${zonesRaw.count()} locations")

  val puZones = zonesRaw
    .withColumnRenamed("LocationID", "PULocationID")
    .withColumnRenamed("Borough", "PU_Borough")
    .withColumnRenamed("Zone", "PU_Zone")
    .withColumnRenamed("service_zone", "PU_service_zone")

  val doZones = zonesRaw
    .withColumnRenamed("LocationID", "DOLocationID")
    .withColumnRenamed("Borough", "DO_Borough")
    .withColumnRenamed("Zone", "DO_Zone")
    .withColumnRenamed("service_zone", "DO_service_zone")

  // ==============================================================================
  // 2) Lecture brute avec gestion d'erreurs
  // ==============================================================================
  logger.info("Lecture des données brutes...")

  val rawDF: DataFrame = try {
    spark.read.parquet(filesPath)
  } catch {
    case e: Exception =>
      logger.error(s"Erreur critique lors de la lecture des données: ${e.getMessage}")
      spark.stop()
      throw e
  }

  val rawCount = rawDF.count()
  logger.info(s"Lignes brutes lues: ${rawCount}")
  assert(rawCount > 0, "Dataset brut vide!")

  // ==============================================================================
  // 3) Nettoyage contractuel de base
  // ==============================================================================
  logger.info("Application des règles de contrat...")

  val allowedVendors = Seq(1, 2, 6, 7)
  val allowedRatecodes = Seq(1, 2, 3, 4, 5, 6, 99)
  val allowedPaymentTypes = Seq(0, 1, 2, 3, 4, 5, 6)
  val payingTypes = Seq(1, 2)

  val amountCols = Seq(
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "Airport_fee",
    "cbd_congestion_fee", "total_amount"
  )

  val dfFilled = rawDF.na.fill(0.0, amountCols)

  val contractDF = dfFilled
    .filter($"passenger_count".isNotNull)
    .filter($"passenger_count" >= 1 && $"passenger_count" <= 6)
    .filter(year($"tpep_pickup_datetime") >= 2020)
    .filter($"VendorID".isin(allowedVendors: _*))
    .filter($"RatecodeID".isin(allowedRatecodes: _*))
    .filter($"payment_type".isin(allowedPaymentTypes: _*))
    .filter($"PULocationID".isNotNull && $"DOLocationID".isNotNull)
    .filter($"tpep_pickup_datetime".isNotNull && $"tpep_dropoff_datetime".isNotNull)
    .filter($"tpep_dropoff_datetime" >= $"tpep_pickup_datetime")
    .filter($"trip_distance" > 0.0)
    .filter($"store_and_fwd_flag".isNull || $"store_and_fwd_flag".isin("Y", "N"))
    .filter($"fare_amount" >= 0.0)
    .filter($"total_amount" >= 0.0)
    .filter(!$"payment_type".isin(payingTypes: _*) || $"total_amount" > 0.0)

  val afterContractCount = contractDF.count()
  logger.info(s"Après filtres contractuels: $afterContractCount (${rawCount - afterContractCount} supprimées)")

  // ==============================================================================
  // 4) Validation des zones (Détection des locations invalides)
  // ==============================================================================
  logger.info("Validation des LocationIDs...")

  val validLocationIDs = zonesRaw.select("LocationID").distinct().as[Int].collect().toSet

  val invalidPickups = contractDF
    .filter(!$"PULocationID".isin(validLocationIDs.toSeq: _*))
    .select("PULocationID").distinct()

  val invalidDropoffs = contractDF
    .filter(!$"DOLocationID".isin(validLocationIDs.toSeq: _*))
    .select("DOLocationID").distinct()

  if (invalidPickups.count() > 0) {
    logger.warn(s"⚠️ ${invalidPickups.count()} PULocationID invalides détectés")
    invalidPickups.show(false)
  }

  if (invalidDropoffs.count() > 0) {
    logger.warn(s"⚠️ ${invalidDropoffs.count()} DOLocationID invalides détectés")
    invalidDropoffs.show(false)
  }

  val validZonesDF = contractDF
    .filter($"PULocationID".isin(validLocationIDs.toSeq: _*))
    .filter($"DOLocationID".isin(validLocationIDs.toSeq: _*))

  val afterZoneValidation = validZonesDF.count()
  logger.info(s"Après validation zones: $afterZoneValidation (${afterContractCount - afterZoneValidation} supprimées)")

  // ==============================================================================
  // 5) NOUVEAU: Filtrage des tips aberrants (tip > total - tip)
  // ==============================================================================
  logger.info("Filtrage des tips aberrants...")

  // Règle: tip_amount > (total_amount - tip_amount)
  // Simplifié: tip_amount > total_amount / 2
  // Signifie que le tip représente plus de 50% du total (aberrant)

  val validTipsDF = validZonesDF
    .filter(
      $"tip_amount".isNull ||
        $"tip_amount" <= ($"total_amount" - $"tip_amount") ||
        $"payment_type" =!= 1 // Les paiements non-carte n'ont généralement pas de tip enregistré
    )

  val afterTipFilter = validTipsDF.count()
  val tipsFiltered = afterZoneValidation - afterTipFilter
  logger.info(s"Après filtre tips aberrants: $afterTipFilter ($tipsFiltered tips > (total-tip) supprimés)")

  // ==============================================================================
  // 6) Détection et suppression des remboursements (robuste avec total_amount)
  // ==============================================================================
  logger.info("Détection des remboursements (robuste aux décimales et total_amount)...")

  // Arrondir distance et total_amount pour éviter les microdifférences
  val withTripKey = validTipsDF.withColumn(
    "trip_key",
    concat_ws("_",
      unix_timestamp($"tpep_pickup_datetime"),      // timestamps en secondes
      unix_timestamp($"tpep_dropoff_datetime"),
      round($"trip_distance", 2),                   // arrondi distance à 2 décimales
      round($"total_amount", 2),                    // arrondi total_amount à 2 décimales
      $"PULocationID",
      $"DOLocationID",
      $"VendorID"
    )
  )

  // Groupement par clé pour détecter les remboursements
  val tripGroups = withTripKey
    .groupBy("trip_key")
    .agg(
      count("*").as("count"),
      sum(when($"total_amount" > 0, 1).otherwise(0)).as("positive_count"),
      sum(when($"total_amount" < 0, 1).otherwise(0)).as("negative_count")
    )

  // Récupération des trip_keys ayant à la fois positif et négatif
  val refundKeys = tripGroups
    .filter($"positive_count" > 0 && $"negative_count" > 0)
    .select("trip_key")

  val nbRefunds = refundKeys.count()
  logger.info(s"Remboursements détectés: $nbRefunds groupes")

  // Suppression des remboursements
  val noRefundsDF = withTripKey
    .join(refundKeys, Seq("trip_key"), "left_anti")
    .drop("trip_key")

  val afterRefunds = noRefundsDF.count()
  logger.info(s"Après suppression remboursements: $afterRefunds (${validTipsDF.count() - afterRefunds} lignes supprimées)")

  // ==============================================================================
  // 7) Déduplication complète
  // ==============================================================================
  logger.info("Déduplication des enregistrements...")

  val deduplicatedDF = noRefundsDF.dropDuplicates(Seq(
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "total_amount", "VendorID", "passenger_count"
  ))

  val afterDedup = deduplicatedDF.count()
  val duplicatesRemoved = afterRefunds - afterDedup
  logger.info(s"Après déduplication: $afterDedup ($duplicatesRemoved duplicatas supprimés)")

  // ==============================================================================
  // 8) Calcul vitesse & durée
  // ==============================================================================
  logger.info("Calcul de la vitesse et de la durée...")

  val withDuration = deduplicatedDF.withColumn(
    "trip_duration_hours",
    (unix_timestamp($"tpep_dropoff_datetime") - unix_timestamp($"tpep_pickup_datetime")) / 3600.0
  )

  val withSpeed = withDuration.withColumn(
    "avg_speed_mph",
    when($"trip_duration_hours" > 0.0, $"trip_distance" / $"trip_duration_hours").otherwise(lit(null))
  )

  val minDurationHours = config("MIN_DURATION_MINUTES") / 60.0
  val maxSpeedMphHard = config("MAX_SPEED_MPH_HARD")

  val speedFilteredDF = withSpeed
    .filter($"avg_speed_mph".isNotNull)
    .filter($"trip_duration_hours" >= minDurationHours)
    .filter($"avg_speed_mph" <= maxSpeedMphHard)

  val afterSpeedFilter = speedFilteredDF.count()
  logger.info(s"Après filtres vitesse/durée: $afterSpeedFilter (${afterDedup - afterSpeedFilter} supprimées)")
  // ==============================================================================
  // 9) Nettoyage Statistique par Route (Z-Score & Boucles)
  // ==============================================================================
  logger.info("Calcul des statistiques avancées par route pour détection d'anomalies...")

  // 1. Calcul de la moyenne et de l'écart-type par trajet Pickup -> Dropoff
  val routeStats = speedFilteredDF
    .groupBy("PULocationID", "DOLocationID")
    .agg(
      avg("trip_distance").as("avg_dist_route"),
      stddev("trip_distance").as("stddev_dist_route"),
      count("*").as("trip_count")
    )
    // On ne fait de stats fiables que sur les routes ayant au moins 10 trajets
    .filter($"trip_count" >= config("MIN_TRIP_COUNT_PER_ROUTE"))

  logger.info(s"Routes analysées pour nettoyage statistique: ${routeStats.count()}")

  // 2. Jointure et application des filtres de "vraisemblance"
  val distanceFilteredDF = speedFilteredDF
    .join(routeStats, Seq("PULocationID", "DOLocationID"), "left")
    .filter {
      // Règle A : Sécurité sur les boucles (Même zone de départ et d'arrivée)
      // Un trajet PU == DO ne devrait pas faire 60 miles.
      when($"PULocationID" === $"DOLocationID", $"trip_distance" < 5.0)
        .otherwise(lit(true))
    }
    .filter {
      // Règle B : Filtrage par Z-Score (3 écarts-types)
      // On tolère une marge si l'écart-type est très faible, mais on dégage les extrêmes.
      // Si stddev est nul (un seul type de trajet), on garde par défaut.
      $"stddev_dist_route".isNull || $"stddev_dist_route" === 0 ||
        (abs($"trip_distance" - $"avg_dist_route") <= (lit(3.0) * $"stddev_dist_route"))
    }
    .filter {
      // Règle C : Seuil minimum réaliste (évite les 0.01 miles du tableau)
      $"trip_distance" >= 0.3
    }
    .drop("avg_dist_route", "stddev_dist_route", "trip_count")

  val afterDistanceFilter = distanceFilteredDF.count()
  val distanceFiltered = afterSpeedFilter - afterDistanceFilter
  logger.info(s"Après nettoyage statistique des routes: $afterDistanceFilter ($distanceFiltered trajets incohérents supprimés)")

  // Affichage des nouvelles stats globales pour vérification
  val globalStats = distanceFilteredDF.agg(
    min("trip_distance").as("global_min"),
    max("trip_distance").as("global_max"),
    avg("trip_distance").as("global_avg")
  ).collect()(0)

  logger.info(f"=== NOUVELLES DISTANCES (NETTOYÉES) ===")
  logger.info(f"  Min: ${globalStats.getDouble(0)}%.2f miles")
  logger.info(f"  Max: ${globalStats.getDouble(1)}%.2f miles")
  logger.info(f"  Avg: ${globalStats.getDouble(2)}%.2f miles")

  // ==============================================================================
  // 10) Tip ratio
  // ==============================================================================
  val baseDF = distanceFilteredDF.withColumn(
    "tip_ratio",
    when($"total_amount" > 0.0, $"tip_amount" / $"total_amount").otherwise(lit(null))
  )

  // ==============================================================================
  // 11) Calcul des bornes IQR (Standard Rate uniquement)
  // ==============================================================================
  logger.info("Calcul des bornes IQR pour détection outliers...")

  val standardTripsDF = baseDF.filter($"RateCodeID" === 1)

  val iqrCols = Seq("trip_distance", "fare_amount", "total_amount", "tip_amount",
    "avg_speed_mph", "trip_duration_hours", "tip_ratio")
  val quantileProbs = Array(0.25, 0.75)
  val relError = 0.01

  val iqrBounds: Map[String, (Double, Double)] = iqrCols.map { colName =>
    val quantiles: Array[Double] = standardTripsDF.stat.approxQuantile(colName, quantileProbs, relError)
    val q1 = quantiles(0)
    val q3 = quantiles(1)
    val iqr = q3 - q1
    val lower = q1 - 1.5 * iqr
    val upper = q3 + 1.5 * iqr
    logger.info(f"  $colName: IQR=[$q1%.2f, $q3%.2f] → Bounds=[$lower%.2f, $upper%.2f]")
    colName -> (lower, upper)
  }.toMap

  // ==============================================================================
  // 12) Enrichissement Zones
  // ==============================================================================
  val withZones = baseDF
    .join(puZones, Seq("PULocationID"), "left")
    .join(doZones, Seq("DOLocationID"), "left")

  // ==============================================================================
  // 13) Définition des règles d'outliers
  // ==============================================================================
  logger.info("Application des règles de détection d'outliers...")

  val (distLow, distHigh) = iqrBounds("trip_distance")
  val (fareLow, fareHigh) = iqrBounds("fare_amount")
  val (totalLow, totalHigh) = iqrBounds("total_amount")
  val (tipAmtLow, tipAmtHigh) = iqrBounds("tip_amount")
  val (speedLow, speedHigh) = iqrBounds("avg_speed_mph")
  val (durLow, durHigh) = iqrBounds("trip_duration_hours")
  val (tipRatioLow, tipRatioHigh) = iqrBounds("tip_ratio")

  val withOutlierFlags = withZones
    .withColumn("is_airport_trip",
      $"PU_service_zone" === "Airports" || $"DO_service_zone" === "Airports")
    .withColumn("is_yellow_zone_trip",
      $"PU_service_zone" === "Yellow Zone" && $"DO_service_zone" === "Yellow Zone")
    .withColumn("price_per_mile",
      when($"trip_distance" > 0.1, $"total_amount" / $"trip_distance").otherwise(lit(0.0)))

    // Flags IQR
    .withColumn("out_trip_distance_iqr", $"trip_distance" < distLow || $"trip_distance" > distHigh)
    .withColumn("out_total_amount_iqr", $"total_amount" < totalLow || $"total_amount" > totalHigh)
    .withColumn("out_avg_speed_iqr", $"avg_speed_mph" < speedLow || $"avg_speed_mph" > speedHigh)
    .withColumn("out_duration_iqr", $"trip_duration_hours" < durLow || $"trip_duration_hours" > durHigh)

    // Règles métier
    .withColumn("out_price_per_mile_aberrant",
      ($"trip_distance" > 0.5) &&
        ($"price_per_mile" > config("MAX_PRICE_PER_MILE")) &&
        (!$"is_airport_trip")
    )
    .withColumn("out_expensive_tiny_trip",
      ($"trip_distance" < config("TINY_DISTANCE_THRESHOLD")) &&
        ($"total_amount" > config("MAX_PRICE_FOR_TINY_DIST"))
    )
    .withColumn("out_speed_excessive",
      ($"is_yellow_zone_trip" && $"avg_speed_mph" > config("SPEED_YELLOW_ZONE_THRESHOLD")) ||
        (!$"is_yellow_zone_trip" && $"avg_speed_mph" > config("SPEED_GLOBAL_SOFT_THRESHOLD"))
    )
    .withColumn("out_duration_too_long", $"trip_duration_hours" > config("MAX_DURATION_HOURS"))
    .withColumn("out_negative_values", $"total_amount" < 0.0 || $"fare_amount" < 0.0)

    // Décision finale
    .withColumn("is_outlier",
      $"out_price_per_mile_aberrant" ||
        $"out_expensive_tiny_trip" ||
        $"out_speed_excessive" ||
        $"out_duration_too_long" ||
        $"out_negative_values" ||
        (($"out_total_amount_iqr" && $"out_trip_distance_iqr") && ($"price_per_mile" > 20.0))
    )

  // ==============================================================================
  // 14) Métadonnées de traçabilité
  // ==============================================================================
  val processingTimestamp = current_timestamp()
  val pipelineVersionLit = lit(pipelineVersion)

  val withMetadata = withOutlierFlags
    .withColumn("processing_timestamp", processingTimestamp)
    .withColumn("pipeline_version", pipelineVersionLit)
    .withColumn("data_quality_score",
      when($"is_outlier", lit(0))
        .when($"out_trip_distance_iqr" || $"out_total_amount_iqr", lit(75))
        .otherwise(lit(100))
    )

  // ==============================================================================
  // 15) Préparation DWH
  // ==============================================================================
  logger.info("Préparation du DataFrame pour le DWH...")

  def reason(colName: String, reasonTag: String) =
    when(col(colName), lit(reasonTag)).otherwise(lit(null))

  val dwhReadyDF = withMetadata
    .withColumn("date_id", date_format($"tpep_pickup_datetime", "yyyyMMdd").cast("int"))
    .withColumn("trip_duration_minutes", $"trip_duration_hours" * 60.0)
    .withColumn("outlier_reasons_array", array(
      reason("out_speed_excessive", "High Speed"),
      reason("out_price_per_mile_aberrant", "Price/Mile > $25"),
      reason("out_expensive_tiny_trip", "Expensive Tiny Trip"),
      reason("out_duration_too_long", "Duration > 5h"),
      reason("out_negative_values", "Negative Amount")
    ))
    .withColumn("outlier_reason",
      concat_ws(", ", expr("filter(outlier_reasons_array, x -> x is not null)"))
    )
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
      $"is_outlier", $"outlier_reason",
      $"data_quality_score", $"processing_timestamp", $"pipeline_version"
    )

  // ==============================================================================
  // 16) Rapport de qualité des données
  // ==============================================================================
  logger.info("\n" + "=" * 80)
  logger.info("RAPPORT QUALITÉ DES DONNÉES")
  logger.info("=" * 80)

  val qualityMetrics = dwhReadyDF.agg(
    count("*").as("total_rows"),
    sum(when($"is_outlier", 1).otherwise(0)).as("outliers"),
    sum(when($"data_quality_score" === 100, 1).otherwise(0)).as("perfect_quality"),
    sum(when($"data_quality_score" === 75, 1).otherwise(0)).as("good_quality"),
    avg("trip_distance").as("avg_distance"),
    avg("total_amount").as("avg_amount"),
    stddev("trip_distance").as("stddev_distance"),
    stddev("total_amount").as("stddev_amount")
  ).collect()(0)

  val totalRows = qualityMetrics.getLong(0)
  val outliers = qualityMetrics.getLong(1)
  val perfectQuality = qualityMetrics.getLong(2)
  val goodQuality = qualityMetrics.getLong(3)

  logger.info(f"Total de courses: $totalRows%,d")
  logger.info(f"  - Qualité parfaite (score 100): $perfectQuality%,d (${perfectQuality * 100.0 / totalRows}%.2f%%)")
  logger.info(f"  - Bonne qualité (score 75): $goodQuality%,d (${goodQuality * 100.0 / totalRows}%.2f%%)")
  logger.info(f"  - Outliers (score 0): $outliers%,d (${outliers * 100.0 / totalRows}%.2f%%)")
  logger.info(f"Distance: Avg=${qualityMetrics.getDouble(4)}%.2f miles, StdDev=${qualityMetrics.getDouble(6)}%.2f")
  logger.info(f"Montant: Avg=$$${qualityMetrics.getDouble(5)}%.2f, StdDev=$$${qualityMetrics.getDouble(7)}%.2f")

  // ==============================================================================
  // 17) Tests de régression (Data Quality Assertions)
  // ==============================================================================
  logger.info("\n" + "=" * 80)
  logger.info("TESTS DE RÉGRESSION")
  logger.info("=" * 80)

  try {
    // Test 1: Dataset non vide
    assert(totalRows > 0, "❌ CRITIQUE: Dataset DWH vide!")
    logger.info("✅ Test 1 OK: Dataset non vide")

    // Test 2: Filtrage effectué
    assert(totalRows < rawCount, "❌ CRITIQUE: Aucun filtrage appliqué!")
    logger.info(s"✅ Test 2 OK: Filtrage effectué (${rawCount - totalRows} lignes supprimées)")

    // Test 3: Pas de montants négatifs
    val negativeAmounts = dwhReadyDF.filter($"total_amount" < 0).count()
    assert(negativeAmounts == 0, s"❌ ERREUR: $negativeAmounts montants négatifs trouvés!")
    logger.info("✅ Test 3 OK: Aucun montant négatif")

    // Test 4: Pas de durées nulles ou négatives
    val invalidDurations = dwhReadyDF.filter($"trip_duration_minutes" <= 0).count()
    assert(invalidDurations == 0, s"❌ ERREUR: $invalidDurations durées invalides!")
    logger.info("✅ Test 4 OK: Toutes les durées sont valides")

    // Test 5: Pas de vitesses aberrantes
    val excessiveSpeeds = dwhReadyDF.filter($"avg_speed_mph" > maxSpeedMphHard).count()
    assert(excessiveSpeeds == 0, s"❌ ERREUR: $excessiveSpeeds vitesses > $maxSpeedMphHard mph!")
    logger.info(s"✅ Test 5 OK: Aucune vitesse > $maxSpeedMphHard mph")

    // Test 6: Toutes les lignes ont les métadonnées
    val missingMetadata = dwhReadyDF.filter($"processing_timestamp".isNull || $"pipeline_version".isNull).count()
    assert(missingMetadata == 0, s"❌ ERREUR: $missingMetadata lignes sans métadonnées!")
    logger.info("✅ Test 6 OK: Toutes les lignes ont les métadonnées")

    // Test 7: Qualité minimum atteinte
    val qualityThreshold = 0.70 // 70% des données doivent être de bonne qualité
    val goodDataRatio = (perfectQuality + goodQuality).toDouble / totalRows
    assert(goodDataRatio >= qualityThreshold,
      f"❌ ERREUR: Qualité insuffisante ($goodDataRatio%.2f < $qualityThreshold)")
    logger.info(f"✅ Test 7 OK: Ratio de qualité = $goodDataRatio%.2f >= $qualityThreshold")

    logger.info("\n🎉 TOUS LES TESTS PASSÉS AVEC SUCCÈS!")

  } catch {
    case e: AssertionError =>
      logger.error(s"❌ ÉCHEC DES TESTS: ${e.getMessage}")
      spark.stop()
      throw e
  }


  // ==============================================================================
  // 19) Résumé complet normal vs outliers
  // ==============================================================================
  logger.info("\n" + "=" * 80)
  logger.info("RÉSUMÉ DÉTAILLÉ DES OUTLIERS ET DES DONNÉES NORMALES")
  logger.info("=" * 80)

  val metrics = Seq(
    "trip_distance", "avg_speed_mph", "total_amount", "fare_amount", "tip_amount"
  )

  metrics.foreach { colName =>
    // Normal rows
    val normalStats = dwhReadyDF.filter(!$"is_outlier").agg(
      min(col(colName)).as("min_val"),
      max(col(colName)).as("max_val")
    ).collect()(0)

    // Outliers
    val outlierStats = dwhReadyDF.filter($"is_outlier").agg(
      min(col(colName)).as("min_val"),
      max(col(colName)).as("max_val")
    ).collect()(0)

    println(f"\n=== $colName ===")
    println(f"Normal : min=${normalStats.getDouble(0)}%.2f, max=${normalStats.getDouble(1)}%.2f")
    println(f"Outliers: min=${outlierStats.getDouble(0)}%.2f, max=${outlierStats.getDouble(1)}%.2f")
  }

  // Statistiques globales supplémentaires
  val totalRowsFinal = dwhReadyDF.count()
  val outliersFinal = dwhReadyDF.filter($"is_outlier").count()
  println("\n=== RÉSUMÉ GLOBAL ===")
  println(f"Total lignes: $totalRowsFinal%,d")
  println(f"Outliers détectés: $outliersFinal%,d (${outliersFinal * 100.0 / totalRowsFinal}%.2f%%)")
  println(f"Lignes normales: ${totalRowsFinal - outliersFinal}%,d")
  println("=" * 80 + "\n")



  // ==============================================================================
  // 18) Écriture avec partitionnement et gestion d'erreurs
  // ==============================================================================
  logger.info("\n" + "=" * 80)
  logger.info("ÉCRITURE DES DONNÉES")
  logger.info("=" * 80)

  // Exemple de stratégie d'écriture robuste
  try {
    logger.info("Écriture DWH principal avec partitionnement par date...")
    dwhReadyDF
      .write
      .mode("overwrite") // Ou "append" selon ton besoin
      .partitionBy("date_id")
      .parquet(s"s3a://$bucketName/dwh/yellow_taxi_refined/")
    val durationMin = (System.currentTimeMillis() - pipelineStartTime) / 60000.0
    logger.info(f"✅ Pipeline terminé avec succès en $durationMin%.2f min")
  } catch {
    case e: Exception =>
      logger.error(s"❌ Erreur lors de l'écriture finale: ${e.getMessage}")
      throw e
  }
  spark.stop()
}