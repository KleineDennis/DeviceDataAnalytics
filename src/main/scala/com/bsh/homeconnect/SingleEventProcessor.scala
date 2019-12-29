package com.bsh.homeconnect

import org.apache.spark.sql.SparkSession

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
object SingleEventProcessor extends App {

  val spark = SparkSession
    .builder
    .appName("SingleEventProcessor")
    .getOrCreate()

  import spark.implicits._

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "raw-events")
    .load()

  val ds = df
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]



  val query = ds.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "enriched-events")
    .start()

  query.awaitTermination()

}
