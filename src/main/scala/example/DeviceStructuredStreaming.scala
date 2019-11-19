package example

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

object DeviceStructuredStreaming extends App  {

  val spark = SparkSession
    .builder
    .appName("DeviceStructuredStreaming")
    .master("local[*]")
    .getOrCreate()

  val path = args(0)
  val ds = spark
    .readStream
    .schema(Device.schema) //Schema must be specified when creating a streaming source DataFrame
    .json(path)

  val splitter = new Splitter("resource")
  splitter.setMessageType("/ro/values") //TODO: define setMessageType(s), parameter with more then one MessageType

  val pipeline = new Pipeline("inbox-split")
    .setStages(Array(splitter))

  // save this unfit pipeline to disk
  pipeline.write.overwrite().save("src/main/resources/inbox-split-pipeline")
  val samePipeline = Pipeline.load("src/main/resources/inbox-split-pipeline")

  val model = pipeline.fit(ds)

  // save the fitted pipeline to disk
  model.write.overwrite().save("src/main/resources/inbox-split-model")
  val sameModel = PipelineModel.load("src/main/resources/inbox-split-model")

  val dataframe = model.transform(ds)

  val query = dataframe
    .writeStream
    .outputMode("append") //update, complete, append
    .format("console")
    .option("checkpointLocation", "src/main/resources/checkpoint")
    .start()

  query.awaitTermination()

//  spark.stop()
}
