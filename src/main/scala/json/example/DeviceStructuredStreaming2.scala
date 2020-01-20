package json.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object DeviceStructuredStreaming2 extends App {
  val spark = SparkSession
    .builder
    .appName("DeviceStructuredStreaming")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val data = ArrayType(
    StructType(
      StructField("uid", StringType, true) ::
      StructField("value", StringType, true) ::
      Nil
    )
  )

  val message =
    StructType(
      StructField("sID", LongType, true) ::
      StructField("msgID", LongType, true) ::
      StructField("resource", StringType, true) ::
      StructField("data", data, true) ::
      Nil
    )

  val deviceSchema =
    StructType(
      StructField("VID", StringType, true) ::
      StructField("messageKey", StringType, true) ::
      StructField("ROProfil", StringType, true) ::
      StructField("EvtDateTime", StringType, true) ::
      StructField("Document", message, true) ::
      Nil
    )

  val path = "src/main/resources/export"
  val ds = spark
    .readStream
    .schema(deviceSchema)
    .json(path)
    .as[DeviceData]

//  df.printSchema()
//  df.show(false)

  case class Event(uid: String, value: String)
  case class Message(sID: Long, msgID: Long, resource: String, data: Seq[Event] = Seq.empty)
  case class DeviceData(vid: String, messageKey: String, roProfil: String, evtDateTime: String, document: Message)

//  val ds: Dataset[DeviceData] = df.as[DeviceData]
//  ds.show(false)

//  val res1 = df.select('sID).where("sID = 1416426976")
  val res1 = ds.filter(_.document.sID == 2345426976L).map(_.document.sID)      // using typed APIs
  val res2 = ds.filter(_.document.sID == 2345426976L)                        // using typed APIs

  import org.apache.spark.sql.expressions.scalalang.typed
  val res3 = ds.groupByKey(_.document.resource).agg(typed.avg(_.document.msgID)) // using typed API

//  res1.show(false)
//  res2.show(false)
//  res3.show(false)

  val query = res3.writeStream
    .outputMode("complete") //complete
    .format("console")
    .start()

  query.awaitTermination()


//  spark.stop()
}
