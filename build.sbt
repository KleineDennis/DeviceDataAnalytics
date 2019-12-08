import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val sparkVersion = "3.0.0-preview"
lazy val playVersion = "2.8.0"

lazy val root = (project in file("."))
  .settings(
    name := "device-data-analytics",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += sparkTest % Test,
    libraryDependencies += "org.apache.spark"  %% "spark-core"  % sparkVersion,
    libraryDependencies += "org.apache.spark"  %% "spark-sql"   % sparkVersion,
    libraryDependencies += "org.apache.spark"  %% "spark-mllib" % sparkVersion,
    libraryDependencies += "com.typesafe.play" %% "play-json"   % playVersion,
  )
