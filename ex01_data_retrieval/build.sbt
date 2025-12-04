import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "ex01_data_retrieval",

    // Classe principale pour run / assembly
    Compile / mainClass := Some("SparkApp"),
    assembly / mainClass := Some("SparkApp"),
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case _                            => MergeStrategy.first
    }
  )

// Dépendances (même style qu’avant)
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.5" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "3.5.5" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"
