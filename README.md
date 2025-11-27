Le code minimal pour faire fonctionner un code avec Minio :
```scala
import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkApp extends App {
  val spark = SparkSession.builder()
    .appName("SparkApp")
    .master("local")
    .config("fs.s3a.access.key", "minio")
    .config("fs.s3a.secret.key", "minio123")
    .config("fs.s3a.endpoint", "http://localhost:9000/") // A changer lors du déploiement
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "6000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

}
```
