package json.example

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.FunSuite
import play.api.libs.json._


class DeviceSchemaValidationJsonSpec extends FunSuite
  with SharedSparkContext
  with DataFrameSuiteBase {

  test("The Splitter should filter valid MessageTypes") {

    val in = "src/test/resources/inbox/"
    val out = "src/test/resources/outbox/"
    val exp = "src/test/resources/expect/"

    import spark.implicits._
    val dataset = spark
      .read
      .schema(DeviceSchema.document)
      .json(in) //per field one column, Document is one column with json structure and "data" as an Array of Objects


    dataset.printSchema()
    dataset.show(20,false)

    //    val result = dataset.map{ str =>
    //      val json = Json.parse(str)
    //      val resource = (json \ "Document" \ "resource").validate[String]
    ////      val header = json("Document").validate[Header](DeviceJson.headerReads)
    //      val validated = resource match {
    //        case JsSuccess(h, _) if h.equals("/ro/values") => json.toString()
    //        case JsSuccess(h, _) if h.equals("/ro/descriptionChange") => json.toString()
    //        case JsSuccess(h, _) => "Undefined: " + json.toString()
    //        case e: JsError =>  "Errors: " + JsError.toJson(e).toString()
    //      }

    //      val deviceData = json.validate[DeviceData](DeviceJson.deviceDataReads)
    //      val validated = deviceData match {
    //        case JsSuccess(deviceData, _) => {
    //          val _: DeviceData = deviceData
    //          // do something with place
    //          row
    //        }
    //        case e: JsError => {
    //          // error handling flow
    //          "Errors: " + JsError.toJson(e).toString()
    //        }
    //      }

    //      validated
    //    }

    //    result.show(false)
    //    result.foreach(println)

    //    val res = spark.read.schema(DeviceSchema.document)json(result)
    //    res.show(false)
  }
}


