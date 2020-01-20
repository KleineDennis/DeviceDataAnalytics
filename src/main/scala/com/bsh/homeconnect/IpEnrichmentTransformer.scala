package com.bsh.homeconnect

import java.io.FileNotFoundException

import cats.effect.IO
import com.snowplowanalytics.maxmind.iplookups.CreateIpLookups
import com.snowplowanalytics.maxmind.iplookups.model.IpLookupResult
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{DataType, StringType}
import org.json4s.ParserUtil.ParseException
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.{write => swrite}
import org.json4s.{DefaultFormats, JValue, MappingException}

/**
 * SingleEventProcessor, which handles validation, enrichment and routing.
 *
 * - 1. Look for shopper IP address (Found|Missing->BadEvent)
 * - 2. Look up geo-location (Found|Error->BadEvent)
 * - 3. Add geo-location to event (Error->BadEvent)
 *
 * valid event: - Contains a shopper.ipAddress property, which is a string
 *              - Allows us to add a shopper.country property, which is also a string, without throwing an exception
 *
 * error: generate an error message in JSON format and write this to the bad-events topic --> { "error": "Something went wrong" }
 */
class IpEnrichmentTransformer(override val uid: String)
  extends UnaryTransformer[String, String, IpEnrichmentTransformer] with DefaultParamsWritable {

  final val geoFile: Param[String] = new Param(this, "geoFile", "Geographic lookup database filepath")

  def this() = this(Identifiable.randomUID("myT"))
  def getGeoFile: String = $(geoFile)
  def setGeoFile(value: String): this.type = set(geoFile, value)

  override protected def outputDataType: DataType = StringType

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be ${StringType.catalogString} type but got ${inputType.catalogString}.")
  }

  override protected def createTransformFunc: String => String = (input: String) => {
    case class Shopper(id: String, name: String, ipAddress: String, country: Option[String], city: Option[String])
    case class Product(sku: String, name: String)
    case class Event(event: String, shopper: Shopper, product: Product, timestamp: String)
    implicit val formats = DefaultFormats

    //string parsen to JsValue
    def parseEvent(input: String): Either[Exception, JValue] =
      try Right(parse(input))
      catch {
        case e: ParseException => Left(e)
        case e: Exception => Left(new Exception("parsing failed", e))
      }

    //JsValue to Object Event
    def validateEvent(input: JValue): Either[Exception, Event] =
      try Right(input.extract[Event])
      catch {
        case e: MappingException => Left(e)
        case e: Exception => Left(new Exception("unknown error", e))
      }

    //find ipLocation
    def findIpLocation(event: Event): Either[Exception, IpLookupResult] =
      try Right {
        CreateIpLookups[IO].createFromFilenames(
          geoFile = Some(getGeoFile),
          ispFile = None,
          domainFile = None,
          connectionTypeFile = None,
          memCache = false,
          lruCacheSize = 20000
        ).flatMap(ipLookups =>
          ipLookups.performLookups(event.shopper.ipAddress)
            .map(lookup => lookup)
        ).unsafeRunSync()
      }
      catch {
        case e: FileNotFoundException => Left(e)
        case e: Exception => Left(new Exception("unknown error", e))
      }

    //enrichEvent
    def enrichEvent(event: Event, ip: IpLookupResult): Either[Throwable, Event] =
      ip.ipLocation match {
        case Some(Right(loc)) => Right(event.copy(shopper = event.shopper.copy(country = Some(loc.countryName), city = loc.city)))
        case Some(Left(e)) => Left(e)
        case _ => Left(new Exception("unknown error"))
      }

    val result = for {
      value <- parseEvent(input)
      evt <- validateEvent(value)
      ip <- findIpLocation(evt)
      base <- enrichEvent(evt, ip)
    } yield base

    result match {
      case Right(x) => swrite[Event](x)
      case Left(x) => "Error: " + x.getMessage
    }
  }
}


//  override protected def createTransformFunc: String => String = { (input: String) =>
//    implicit val shopperFormat = Json.format[Shopper]
//    implicit val productFormat = Json.format[Product]
//    implicit val eventFormat = Json.format[Event]
//
//    Try(Json.parse(input)) match {
//      case Success(json) =>
//        Json.fromJson[Event](json) match {
//          case JsSuccess(evt, _) =>
//            val idActual = CreateIpLookups[Id]
//              .createFromFilenames(Some(getGeoFile), None, None, None, false, 0)
//              .performLookups(evt.shopper.ipAddress)
//
//            idActual.ipLocation match {
//              case Some(Right(loc)) =>
//                val enriched = evt.copy(shopper = evt.shopper.copy(country = Some(loc.countryName), city = loc.city))
//                Json.stringify(Json.toJson(enriched))
//              case Some(Left(e)) => "Error: " + e.toString
//              case None => "Error: No ip address found"
//            }
//          case e: JsError => "Error: " + e.toString //Json.stringify(JsError.toJson(e))
//        }
//      case Failure(e) => "Error: " + e.toString
//    }
//  }

