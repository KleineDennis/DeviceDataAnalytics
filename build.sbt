import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val sparkVersion = "3.0.0-preview2"
lazy val playVersion = "2.8.1"

lazy val root = (project in file("."))
  .settings(
    name := "device-data-analytics",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += sparkTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core"  % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql"   % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
    libraryDependencies += "com.maxmind.geoip2" % "geoip2" % "2.13.0",
    libraryDependencies += "com.snowplowanalytics" %% "scala-maxmind-iplookups" % "0.6.1",
    libraryDependencies += "com.typesafe.play" %% "play-json" % playVersion,
  )


//    libraryDependencies += "com.sanoma.cda" %% "maxmind-geoip2-scala" % "1.5.5",  //only Scala 2.11
//    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
