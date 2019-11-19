package example

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}


class Splitter (override val uid: String) extends Transformer with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("inbox-splitter"))

  /**
   * Splitter parameter. The statement is provided in string form.
   *
   * @group param
   */
  final val statement: Param[String] = new Param[String](this, "types", "HCA Message Types")

  /** @group setParam */
  def setMessageType(value: String): this.type = set(statement, value)

  /** @group getParam */
  def getMessageType: String = $(statement)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val spark = dataset.sparkSession
    import spark.implicits._

    val ds = dataset.as[DeviceData]
    val result = ds.filter(_.document.resource == getMessageType) // using typed APIs

    //TODO: Try not to convert explicitly to a DataFrame
    result.toDF()
  }

  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): Splitter = defaultCopy(extra)
}


object Splitter extends DefaultParamsReadable[Splitter] {

  override def load(path: String): Splitter = super.load(path)
}
