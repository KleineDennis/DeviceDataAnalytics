package json.example

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.FunSuite


class DeviceDataFrameEqualsSpec extends FunSuite
  with SharedSparkContext
  with DataFrameSuiteBase {

  test("The Splitter should filter valid MessageTypes") {
    val in = "src/test/resources/inbox/"
    val out = "src/test/resources/outbox/"
    val exp = "src/test/resources/expect/"

    import spark.implicits._

    val actual = spark
      .read
      .text(out)
      .sort($"value")

    val expect = spark
      .read
      .text(exp)
      .sort($"value")

    assertDataFrameEquals(actual, expect)
  }
}
