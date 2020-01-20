package spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.ProcessingTimeTimeout
import org.apache.spark.sql.streaming.GroupState
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}


object AbandonedCartsExample extends App {
  case class UserStatus(userId: String, active: Boolean)
  case class UserAction(userId: String, action: String)

  val spark = SparkSession
    .builder
    .appName("AbandonedCartsProcessor")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val ds = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "raw-events")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  implicit val formats = DefaultFormats
  val events = ds.map { case (key, value) => read[UserAction](value) }

  val sessionUpdates = events
    .groupByKey(event => event.userId)
    .mapGroupsWithState(ProcessingTimeTimeout)(updateState)
    .map(e => write(e))



  val query = sessionUpdates
    .writeStream
    .outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "src/main/resources/checkpoint")
    .option("topic", "test")
    //    .trigger(Trigger.Continuous(1 seconds))
    .start()

  def updateState(userId: String, actions: Iterator[UserAction], state: GroupState[UserStatus]) = {

    val userStatus =

      if (state.hasTimedOut) {
        // handle timeout
        val stat = UserStatus(userId, false)
        state.remove()
        stat
      } else {
        // handle update or new data

        //Get previous user status
        val prevStatus = state.getOption.getOrElse{
          UserStatus(userId, true)    //new Cart
        }

        //Update user status with actions   //add Item to Cart
        //        actions.foreach{ action =>
        //          prevStatus.updateWith(action)
        //        }

        //Update state with latest user status   //update 30s reset abandoned cart
        state.update(prevStatus)

        state.setTimeoutDuration("130 seconds")

        prevStatus
      }

    //return the status
    userStatus
  }

  query.awaitTermination()
}
