package example

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SaveMode}
import org.scalatest.FunSuite


class DeviceStructuredStreamingSpec extends FunSuite
  with SharedSparkContext
  with DataFrameSuiteBase {

  test("The Splitter should filter valid MessageTypes") {
    val in  = "src/test/resources/inbox/"
    val out = "src/test/resources/outbox/"
    val exp = "src/test/resources/expect/"

    import spark.implicits._
    val dataset = spark
      .read
      .schema(DeviceSchema.document)
      .json(in)
      .as[DeviceData]

    val ds = dataset.filter(col("Document.sID").isNotNull)
    val devices = ds.filter(_.document.sID == 1304512619) //.map(_.document)

    devices.show(false)

    def deviceFilter(d: DeviceData): Boolean = {
      val events = d.document.data
      val res = events.exists(e => e.uid.equals("BSH.Common.Option.RemainingProgramTime") && e.value.toInt > 1000)
      res
    }

    val filtered = devices.filter(deviceFilter(_))
    filtered.show(false)



//    val doc = dataset.filter(p => p.document.sID == 1304512619)
//    val data = dataset.map(_.document.data)
//    doc.show(false)
//    data.show(false)

//    val doc = dataset.select($"document", $"document.sID")//.where($"document.sID")
//    doc.count()
//    doc.show()

//    val ds  = dataset.filter(_.roProfil.contains("HA"))
//    ds.show()
//
//    val splitter = new Splitter()
//    splitter.setMessageTypes(Seq("/ro/values","/ci/info","/ci/services"))
//
//    val result = new Pipeline()
//      .setStages(Array(splitter))
//      .fit(ds)
//      .transform(ds)
//      .as[DeviceData]


//    val res = doc.map(p => (p.sID, p.msgID)).toDF("sID", "msgID")
//    res.show()

//    res
//      .write
//      .mode(SaveMode.Overwrite)
//      .parquet(out)
//      .text("out")

    //    /////////////////////////////////
//    val actual = spark
//      .read
//      .json(in)

//
//    val expect = spark
//      .read
//      .json(exp)
//
//    assertDataFrameEquals(actual, expect)
  }

  test("Invoking head on an empty Set should produce NoSuchElementException") {
    assertThrows[NoSuchElementException] {
      Set.empty.head
    }
  }
}
