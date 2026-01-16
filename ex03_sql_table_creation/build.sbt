name := "ex03_sql_table_creation"
version := "1.0"
scalaVersion := "2.13.16"

val sparkVersion = "3.5.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.postgresql" % "postgresql" % "42.6.0"
)

// Assembly settings
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyJarName := "ex03-assembly.jar"
