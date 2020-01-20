package json.example

object MaxMindGeoData extends App {

  import cats.effect.IO
  import cats.{Eval, Id}
  import com.snowplowanalytics.maxmind.iplookups.CreateIpLookups

  val geoFile = getClass.getResource("/GeoLite2-City.mmdb").getFile
  val ip = "216.160.83.56"
  val memCache = false
  val lruCache = 0

  val ioActual = CreateIpLookups[IO].createFromFilenames(Some(geoFile), None, None, None, memCache, lruCache)
    .unsafeRunSync
    .performLookups(ip)
    .unsafeRunSync

  val evalActual = CreateIpLookups[Eval]
    .createFromFilenames(Some(geoFile), None, None, None, memCache, lruCache)
    .value
    .performLookups(ip)
    .value

  val idActual = CreateIpLookups[Id]
    .createFromFilenames(Some(geoFile), None, None, None, memCache, lruCache)
    .performLookups(ip)

  idActual.ipLocation match {
    case Some(Right(loc)) =>
      println(loc.countryCode)
      println(loc.countryName)
      println(loc.city.getOrElse(""))
    case _ =>
      println("Lookup failed")
  }


  ////////////////////////////////////////////////////////////////////////////////
  //Java: fasterxml and MaxMind
  ////////////////////////////////////////////////////////////////////////////////
{
  val message = """
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
   """

  import java.io.File
  import java.net.InetAddress

  import com.fasterxml.jackson.databind.ObjectMapper
  import com.fasterxml.jackson.databind.node.ObjectNode
  import com.fasterxml.jackson.module.scala.ScalaObjectMapper
  import com.maxmind.geoip2.DatabaseReader

  val path = "/Users/denniskleine/Downloads/GeoLite2-City.mmdb"
  val maxmind = new DatabaseReader.Builder(new File(path)).build

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  val root = mapper.readTree(message)
  val ipNode = root.path("shopper").path("ipAddress")
  val ip = InetAddress.getByName(ipNode.textValue)
  val resp = maxmind.city(ip)
  val country = resp.getCountry.getName
  val city = resp.getCity.getName
  root.`with`[ObjectNode]("shopper").put("country", country)
  root.`with`[ObjectNode]("shopper").put("city", city)
  val str = mapper.writeValueAsString(root)

  import play.api.libs.json.Json
  println(Json.prettyPrint(Json.parse(str)))
}
}
