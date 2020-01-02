package com.bsh.homeconnect

import cats.Id
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import com.snowplowanalytics.maxmind.iplookups.CreateIpLookups
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, SparkSession}
import play.api.libs.json.{JsError, JsSuccess, Json}

import scala.util.{Failure, Success, Try}

/**
 * SingleEventProcessor, which handles validation, enrichment and routing.
 *
 * - Read events from our Kafka topic raw-events
 * - Validate the events, writing any validation failures to the bad-events Kafka topic
 * - Enrich our validated events with the geographical location of the shopper by using the MaxMind geo-IP database
 * - Write our validated, enriched events to the enriched-events Kafka topic
 *
 * valid event: - Contains a shopper.ipAddress property, which is a string
 *              - Allows us to add a shopper.country property, which is also a string, without throwing an exception
 *
 * error: generate an error message in JSON format and write this to the bad-events topic --> { "error": "Something went wrong" }
 */
object SingleEventProcessor2 extends App {

  val spark = SparkSession
    .builder
    .appName("SingleEventProcessor")
    .getOrCreate()

  import spark.implicits._

  def validate(ds: Dataset[String]): Dataset[String] = {
    val parseUDF = udf((input: String) => {
      Try(Json.parse(input)) match {
        case Failure(e) => "Error: " + e.toString
        case Success(json) =>
          val ip = (json \ "shopper" \ "ipAddress").validate[String]
          ip match {
            case JsSuccess(ip, _) => Json.stringify(json)
            case e: JsError => "Error: " + e.toString
          }
      }
    })
    ds.withColumn("output", parseUDF($"value"))
      .drop($"value")
      .as[String]
  }

  def enrich(ds: Dataset[String]): Dataset[String] = {
    val geoUDF = udf((input: String) => {
      if (input.startsWith("Error") == false)  {
        val geoFile = getClass.getResource("/GeoLite2-City.mmdb").getFile
        val mapper = new ObjectMapper with ScalaObjectMapper
        val root = mapper.readTree(input)
        val ip = root.path("shopper").path("ipAddress").textValue
        val idActual = CreateIpLookups[Id]
          .createFromFilenames(Some(geoFile), None, None, None, false, 0)
          .performLookups(ip)

        idActual.ipLocation match {
          case Some(Right(loc)) =>
            root.`with`[ObjectNode]("shopper").put("country", loc.countryName)
            root.`with`[ObjectNode]("shopper").put("city", loc.city.orNull)
            mapper.writeValueAsString(root)
          case Some(Left(e)) => "Error: " + e.toString
          case _ => "Error: no ip address found"
        }
      } else {
        input
      }
    }, DataTypes.StringType)

    ds.withColumnRenamed("output", "input")
      .withColumn("output", geoUDF($"input"))
      .drop($"input")
      .as[String]
  }

  def filter(ds: Dataset[String]): Dataset[(String, String)] = {
    val topicUDF = udf((input: String) => if (input.startsWith("Error")) "bad-events" else "enriched-events", DataTypes.StringType)
    ds.withColumn("topic", topicUDF($"output"))
      .as[(String, String)]
  }

  ////////////////////////////////////////////////////////////////////////////////////////
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "raw-events")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[String]

  val result = df
    .transform(validate)
    .transform(enrich)
    .transform(filter)
    .selectExpr("topic", "output as value")

  val query = result.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "src/main/resources/checkpoint")
    .start()

  query.awaitTermination()
}


