package com.bsh.homeconnect

import org.apache.spark.sql.SparkSession

object StreamingApp extends App {

  val spark = SparkSession
    .builder
    .appName("StructuredKafkaPipeline")
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



  //1. Look for shopper IP address (Found|Missing->BadEvent)

  //2. Look up geo-location (Found|Error->BadEvent)

  //3. Add geo-location to event (Error->BadEvent)




  val query = ds.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "enriched-events")
    .start()

  query.awaitTermination()
}
