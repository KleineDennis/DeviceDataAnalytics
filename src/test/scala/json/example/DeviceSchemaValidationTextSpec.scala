package json.example

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSuite
import play.api.libs.json._


class DeviceSchemaValidationTextSpec extends FunSuite
  with SharedSparkContext
  with DataFrameSuiteBase {

  test("The Splitter should filter valid MessageTypes") {

    val in = "src/test/resources/inbox/"
    val out = "src/test/resources/outbox/"
    val exp = "src/test/resources/expect/"

    import spark.implicits._
    val dataset = spark
      .read
      .textFile(in) //column "value" with json content as a String

    dataset.printSchema()
    dataset.show(20,false)

    val result = dataset.map{ str =>
      val json = Json.parse(str)
      val p = (json \ "ROProfil").as[String]
      val roProfile = json("ROProfil").validate[String]
      val resource = (json \ "Document" \ "resource").validate[String]
      val device = json.validate[DeviceData](DeviceJson.deviceDataReads)
      device match {
        case JsSuccess(h, _) => (Json.stringify(json), roProfile.getOrElse("undefined"), resource.getOrElse("undefined"))
        case e: JsError => ("Errors: " + JsError.toJson(e).toString, "", "")
      }
    }.toDF("value", "roProfil", "resource")



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
//
//      validated
//    }

    result.show(false)
//    val res = spark.read.schema(DeviceSchema.document).json(result)
//    res.show(false)
  }
}



//case JsSuccess(h, _) if h.equals("/ro/values") => (json.toString, h)
//case JsSuccess(h, _) if h.equals("/ro/descriptionChange") => (json.toString, h)
//val header = json("Document").validate[Header](DeviceJson.headerReads)


