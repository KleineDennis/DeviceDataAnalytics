package com.bsh.homeconnect

import cats.Id
import com.snowplowanalytics.maxmind.iplookups.CreateIpLookups
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{DataType, StringType}
import play.api.libs.json._

import scala.util.{Failure, Success, Try}

case class Shopper(id: String, name: String, ipAddress: String, country: Option[String], city: Option[String])
case class Product(sku: String, name: String)
case class Event(event: String, shopper: Shopper, product: Product, timestamp: String)

/**
 * SingleEventProcessor, which handles validation, enrichment and routing.
 *
 * - 1. Look for shopper IP address (Found|Missing->BadEvent)
 * - 2. Look up geo-location (Found|Error->BadEvent)
 * - 3. Add geo-location to event (Error->BadEvent)
 * - 3. Add geo-location to event (Error->BadEvent)
 *
 * valid event: - Contains a shopper.ipAddress property, which is a string
 *              - Allows us to add a shopper.country property, which is also a string, without throwing an exception
 *
 * error: generate an error message in JSON format and write this to the bad-events topic --> { "error": "Something went wrong" }
 */
class IpEnrichmentTransformer(override val uid: String)
  extends UnaryTransformer[String, String, IpEnrichmentTransformer] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("myT"))

  final val geoFile: Param[String] = new Param(this, "geoFile", "Geographic lookup database filepath")
  def getGeoFile: String = $(geoFile)
  def setGeoFile(value: String): this.type = set(geoFile, value)

  override protected def outputDataType: DataType = StringType
  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be ${StringType.catalogString} type but got ${inputType.catalogString}.")
  }

  override protected def createTransformFunc: String => String = { (input: String) =>
    implicit val shopperFormat = Json.format[Shopper]
    implicit val productFormat = Json.format[Product]
    implicit val eventFormat = Json.format[Event]

    Try(Json.parse(input)) match {
      case Success(json) =>
        Json.fromJson[Event](json) match {
          case JsSuccess(evt, _) =>
            val idActual = CreateIpLookups[Id]
              .createFromFilenames(Some(getGeoFile), None, None, None, false, 0)
              .performLookups(evt.shopper.ipAddress)

            idActual.ipLocation match {
              case Some(Right(loc)) =>
                val enriched = evt.copy(shopper = evt.shopper.copy(country = Some(loc.countryName), city = loc.city))
                Json.stringify(Json.toJson(enriched))
              case Some(Left(e)) => "Error: " + e.toString
              case None => "Error: No ip address found"
            }
          case e: JsError => "Error: " + e.toString //Json.stringify(JsError.toJson(e))
        }
      case Failure(e) => "Error: " + e.toString
    }
  }
}
