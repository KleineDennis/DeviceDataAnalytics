import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "device-data-analytics",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.4",
    libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
