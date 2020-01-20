package json.example

import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._


object JsonTransformers extends App {

  val json = Json.parse(
    """
      {
        "event" : "SHOPPER_VIEWED_PRODUCT",
        "shopper" : {
          "id" : "123",
          "name" : "Jane",
          "ipAddress" : "70.46.123.145"
        },
        "product" : {
          "sku" : "aapl-001",
          "name" : "iPad"
        },
        "timestamp" : "2018-10-15T12:01:35Z"
      }
      """)

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //with Case Class Format
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////
  case class Shopper(id: String, name: String, ipAddress: String, country: Option[String], city: Option[String])
  case class Product(sku: String, name: String)
  case class Nile(event: String, shopper: Shopper, product: Product, timestamp: String)

  implicit val shopperFormat = Json.format[Shopper]
  implicit val productFormat = Json.format[Product]
  implicit val nileFormat = Json.format[Nile]

  val nileFromJson = Json.fromJson[Nile](json)

  nileFromJson match {
    case JsSuccess(n: Nile, path: JsPath) =>
      val shopperEnriched = n.shopper.copy(country = Some("China"), city = Some("Peking"))
      val newNile = n.copy(shopper = shopperEnriched)
      val newNileJson = Json.toJson(newNile)
      println(Json.prettyPrint(newNileJson))

    case e@JsError(_) => println("Errors: " + JsError.toJson(e).toString())
  }


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //with Coast-to-Coast Trasnformers
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////
  val jsonTransformerMap = (__ \ 'shopper).json.update(
    __.read[JsObject].map(_ ++ (Json.obj("country" -> "China", "city" -> "Peking"))))

  Json.prettyPrint(json.transform(jsonTransformerMap).get)


  val jsonTransformerPut = (__ \ 'shopper).json.update(
    (
      (__ \ 'Country).json.put(JsString("China")) and
        (__ \ 'City).json.put(JsString("Peking"))
      ) reduce
  )

  println(Json.prettyPrint(json.transform(jsonTransformerPut).get))


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //with Play Json -- validate and deepMerge
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////
  val ip = (json \ "shopper" \ "ipAddress").validate[String]

  ip match {
    case JsSuccess(ip, _) =>
      val merged = json.as[JsObject] deepMerge Json.obj("shopper" -> Json.obj("country" -> "China", "city" -> "Peking"))
      println(Json.prettyPrint(merged))
      println(Json.stringify(merged))
    case e: JsError => println(s"Errors: ${JsError.toJson(e)}")
  }



}
