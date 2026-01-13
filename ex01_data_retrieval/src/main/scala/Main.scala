import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import java.net.URI
import java.net.URL
import java.io.BufferedInputStream
import scala.util.{Try, Success, Failure}

object AppConfig {
  val MinioAccessKey = "minio"
  val MinioSecretKey = "minio123"
  val MinioEndpoint = "http://localhost:9000"
  val BucketName = "nyc-raw"
  val BaseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data"
}

object Main {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    logger.info("Démarrage du job d'ingestion Data Lake...")

    val spark = SparkSession.builder()
      .appName("NYC Taxi Data Ingestion")
      .master("local[*]")
      .getOrCreate()

    try {
      // Configuration Hadoop pour Minio (S3)
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("fs.s3a.endpoint", AppConfig.MinioEndpoint)
      hadoopConf.set("fs.s3a.access.key", AppConfig.MinioAccessKey)
      hadoopConf.set("fs.s3a.secret.key", AppConfig.MinioSecretKey)
      hadoopConf.set("fs.s3a.path.style.access", "true")
      hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
      hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      hadoopConf.set("fs.s3a.metadatastore.impl", "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore")

      val fs = FileSystem.get(new URI(s"s3a://${AppConfig.BucketName}"), hadoopConf)

      // Sélection des mois à traiter (Fichiers existants)
      val monthsToProcess = Seq("2025-09", "2025-10", "2025-11")

      monthsToProcess.foreach { dateStr =>
        val fileName = s"yellow_tripdata_$dateStr.parquet"
        val sourceUrl = s"${AppConfig.BaseUrl}/$fileName"
        val destPath = new Path(s"/$fileName")

        logger.info(s"Traitement : $fileName")

        downloadAndUploadToMinio(sourceUrl, destPath, fs) match {
          case Success(_) => logger.info(s"SUCCÈS : $fileName ingéré dans Minio.")
          case Failure(e) => logger.error(s"ÉCHEC : $fileName. Raison : ${e.getMessage}")
        }
      }

    } catch {
      case e: Exception => logger.error("Erreur critique du job", e)
    } finally {
      spark.stop()
      logger.info("Arrêt de la session Spark.")
    }
  }

  def downloadAndUploadToMinio(urlStr: String, destPath: Path, fs: FileSystem): Try[Unit] = {
    Try {
      val connection = new URL(urlStr).openConnection()
      val in = new BufferedInputStream(connection.getInputStream)
      val out = fs.create(destPath, true)
      val buffer = new Array[Byte](8192)

      Iterator
        .continually(in.read(buffer))
        .takeWhile(_ != -1)
        .foreach(bytesRead => out.write(buffer, 0, bytesRead))

      out.close()
      in.close()
    }
  }
}