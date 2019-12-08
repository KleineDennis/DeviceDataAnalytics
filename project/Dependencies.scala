import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val sparkTest = "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0"
}
