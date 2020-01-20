import Dependencies._

ThisBuild / scalaVersion     := "2.12.10" //"2.11.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val sparkVersion = "3.0.0-preview2" //"2.4.4"
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

    //AWS
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.10.0",
    libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.10.0",
    libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.452",

    //JSON
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7",
    libraryDependencies += "com.maxmind.geoip2" % "geoip2" % "2.13.0",
    libraryDependencies += "com.snowplowanalytics" %% "scala-maxmind-iplookups" % "0.6.1",

//    libraryDependencies += "com.typesafe.play" %% "play-json" % playVersion,
//    libraryDependencies += "org.apache.hudi" % "hudi-spark-bundle" % "0.5.0-incubating",
  )
