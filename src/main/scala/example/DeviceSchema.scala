package example

import org.apache.spark.sql.types._


case class Event(uid: String, value: String)
case class Header(sID: Long, msgID: Long, resource: String, data: Seq[Event] = Seq.empty)
case class DeviceData(vid: String, messageKey: String, roProfil: String, evtDateTime: String, document: Header)


object Device {

  val data = ArrayType(
    StructType(
//        StructField("uid", StringType, true) ::
//        StructField("value", StringType, true) ::
        StructField("parentUID", StringType, true) ::
        StructField("uid", StringType, true) ::
        StructField("value", StringType, true) ::
        StructField("access", StringType, true) ::
        StructField("available", BooleanType, true) ::
        StructField("brand", StringType, true) ::
        StructField("customerIndex", LongType, true) ::
        StructField("deviceID", StringType, true) ::
        StructField("deviceInfo", StringType, true) ::
        StructField("deviceType", LongType, true) ::
        StructField("eNumber", StringType, true) ::
        StructField("fdString", StringType, true) ::
        StructField("haVersion", StringType, true) ::
        StructField("hwVersion", StringType, true) ::
        StructField("mac", StringType, true) ::
        StructField("serialNumber", StringType, true) ::
        StructField("shipSki", StringType, true) ::
        StructField("swVersion", StringType, true) ::
        StructField("vib", StringType, true) ::
        StructField("default", StringType, true) ::
        StructField("enumType", LongType, true) ::
        StructField("execution", StringType, true) ::
        StructField("max", LongType, true) ::
        StructField("min", LongType, true) ::
        StructField("stepSize", LongType, true) ::
        Nil
    )
  )

  val header =
    StructType(
//        StructField("sID", LongType, true) ::
//        StructField("msgID", LongType, true) ::
//        StructField("resource", StringType, true) ::
        StructField("sID", LongType, true) ::
        StructField("msgID", LongType, true) ::
        StructField("resource", StringType, true) ::
        StructField("version", LongType, true) ::
        StructField("action", StringType, true) ::
        StructField("code", LongType, true) ::
        StructField("data", data, true) ::
        Nil
    )

  val schema =
    StructType(
//        StructField("VID", StringType, true) ::
//        StructField("messageKey", StringType, true) ::
//        StructField("ROProfil", StringType, true) ::
//        StructField("EvtDateTime", StringType, true) ::
        StructField("VID", StringType, true) ::
        StructField("messageKey", StringType, true) ::
        StructField("DocFormat", StringType, true) ::
        StructField("TenantID", StringType, true) ::
        StructField("ROProfil", StringType, true) ::
        StructField("ROName", StringType, true) ::
        StructField("Increment", StringType, true) ::
        StructField("Jobname", StringType, true) ::
        StructField("EvtDateTime", StringType, true) ::
        StructField("TransID", StringType, true) ::
        StructField("MessageType", StringType, true) ::
        StructField("Document", header, true) ::
        Nil
    )
}
