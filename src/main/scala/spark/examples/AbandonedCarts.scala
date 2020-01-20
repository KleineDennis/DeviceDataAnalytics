package spark.examples

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

////////////////////////////////////////////////////////////////////////////////////////////
// The input event (I)
// The arbitrary state to keep (S)
// The output (O) (this type might be the same as the state representation, if suitable)
//
// def mappingFunction(key: K, values: Iterator[I], state: GroupState[S]): O
////////////////////////////////////////////////////////////////////////////////////////////
//case class Shopper(id: String, name: String, ipAddress: String, country: Option[String], city: Option[String])
//case class Product(sku: String, name: String)
//case class Item(product: Product, quantity: Int)
//case class Order(id: String, value: Double)

object AbandonedCarts extends App {
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
  val events = ds.map { case (key, value) => read[Event](value) }

  val sessionUpdates = events
    .groupByKey(event => event.shopperId)
    .flatMapGroupsWithState(OutputMode.Update, GroupStateTimeout.ProcessingTimeTimeout)(flatMappingFunction)
    .map(e => write(e))

  val query = sessionUpdates
    .writeStream
    .outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "src/main/resources/checkpoint")
    .option("topic", "test")
    .start()

  query.awaitTermination()

  // def mappingFunction(key: K, values: Iterator[I], state: GroupState[S]): O
  // def flatMappingFunction(key: K, values: Iterator[I], state: GroupState[S]): Iterator[O]
  def flatMappingFunction(shopperId: String, events: Iterator[Event], state: GroupState[CartState]): Iterator[AbandonedCartEvent] = {

    //timed-out with full Cart => remove state
    if (state.hasTimedOut && state.get.items.nonEmpty) {
      // when the state has a timeout, the events are empty; this validation is only to illustrate that point
      assert(events.isEmpty, "When the state has a timeout, the values are empty")

      //remove session and send final abandonedCartEvent
      val abandonedCartEvent = Iterator(AbandonedCartEvent("SHOPPER_ABANDONED_CART", shopperId, state.get.items, new Timestamp(System.currentTimeMillis)))

      // evict the timed-out state
      state.remove()

      // emit the result of transforming the current state into an output record
      abandonedCartEvent
    } else {
      //all other events are ignored in output state (abandoned output queue)

      // get current state or create a new one if there's no previous state
      val currentState = state.getOption.getOrElse(
        CartState(items = List.empty)
      )

      // enrich the state with the new events
      val updatedState = events.foldLeft(currentState) {
        case (st, ev) =>
          //"SHOPPER_ADDED_ITEM_TO_CART" => add Items
          if (ev.event.equals("SHOPPER_ADDED_ITEM_TO_CART")) {
            st.addItem(ev.items)
          }
          //"SHOPPER_PLACED_ORDER" => Cart has expired
          else if (ev.event.equals("SHOPPER_PLACED_ORDER")) {
            CartState(items = st.items, expired = true)
          }
          else st
      }

      // update the state with the enriched state
      state.update(updatedState)

      // Set timeout such that the session will be expired if no data received for 10 seconds
      if (state.get.expired) state.remove()
      else state.setTimeoutDuration("10 seconds")

      Iterator.empty
    }
  }

  //input event (I)
  case class Event(event: String, shopperId: String, items: List[String], timestamp: Timestamp)

  //state to keep (S)
  case class CartState(items: List[String], expired: Boolean = false) {
    def addItem(items: List[String]): CartState = copy(items = this.items ::: items, expired = this.expired)
  }

  //output (O) (this type might be the same as the state representation (S), if suitable)
  //  "SHOPPER_ABANDONED_CART"
  case class AbandonedCartEvent(event: String, shopperId: String, items: List[String], timestamp: Timestamp)

}
