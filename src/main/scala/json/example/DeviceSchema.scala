package json.example

import org.apache.spark.sql.types._
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._

//Content type VALUES provides information about a value of an UID (/ro/values)
//Get all Description Changes, request a list of all description changes (/ro/allDescriptionChanges)
case class Event(uid: String, value: String)
case class Header(sID: Long, msgID: Long, resource: String, data: Seq[Event] = Seq.empty)
case class DeviceData(vid: String, messageKey: String, roProfil: String, evtDateTime: String, document: Header)


object DeviceJson {

  implicit val eventReads: Reads[Event] = (
      (JsPath \ "uid").read[String](minLength[String](2)) and
      (JsPath \ "value").read[String] //(min(0) keepAnd max(2000))
    )(Event.apply _)

  implicit val headerReads: Reads[Header] = (
      (JsPath \ "sID").read[Long] and
      (JsPath \ "msgID").read[Long] and
      (JsPath \ "resource").read[String] and
      (JsPath \ "data").read[Seq[Event]]
    )(Header.apply _)

  implicit val deviceDataReads: Reads[DeviceData] = (
      (__ \ "VID").read[String](minLength[String](2)) and
      (__ \ "messageKey").read[String] and
      (__ \ "ROProfil").read[String] and
      (__ \ "EvtDateTime").read[String] and
      (__ \ "Document").read[Header]
    )(DeviceData.apply _)

}


object DeviceSchema {

  val data = ArrayType(
    new StructType()
      .add("parentUID", StringType, true)
      .add("uid", StringType, true)
      .add("value", StringType, true)
      .add("access", StringType, true)
      .add("available", BooleanType, true)
      .add("brand", StringType, true)
      .add("customerIndex", LongType, true)
      .add("deviceID", StringType, true)
      .add("deviceInfo", StringType, true)
      .add("deviceType", LongType, true)
      .add("eNumber", StringType, true)
      .add("fdString", StringType, true)
      .add("haVersion", StringType, true)
      .add("hwVersion", StringType, true)
      .add("mac", StringType, true)
      .add("serialNumber", StringType, true)
      .add("shipSki", StringType, true)
      .add("swVersion", StringType, true)
      .add("vib", StringType, true)
      .add("default", StringType, true)
      .add("enumType", LongType, true)
      .add("execution", StringType, true)
      .add("max", LongType, true)
      .add("min", LongType, true)
      .add("stepSize", LongType, true)
  )

  val header = new StructType()
    .add("sID", LongType, true, "Session ID, valid for a specific connection")
    .add("msgID", LongType, true, "A message ID value to allot responses to requests (prevent replay attacks)")
    .add("resource", StringType, true, "The resource which will be accessed")
    .add("version", LongType, true, "The version of a resource")
    .add("action", StringType, true, "The action of the message (GET, POST, RESPONSE, NOTIFY)")
    .add("code", LongType, true, "Error code if an error occurred")
    .add("data", data, true)

  val document = new StructType()
    .add("VID", StringType, true)
    .add("messageKey", StringType, true)
    .add("DocFormat", StringType, true)
    .add("TenantID", StringType, true)
    .add("ROProfil", StringType, true)
    .add("ROName", StringType, true)
    .add("Increment", StringType, true)
    .add("Jobname", StringType, true)
    .add("EvtDateTime", StringType, true)
    .add("TransID", StringType, true)
    .add("MessageType", StringType, true)
    .add("Document", header, true)
}
