package spark.examples

import java.sql.Timestamp

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.immutable.Queue

//input event (I)
case class WeatherEvent(stationId: String,
                        timestamp: Timestamp,
                        location: (Double, Double),
                        pressure: Double,
                        temp: Double)

//state to keep (S)
case class FIFOBuffer[T](capacity: Int, data: Queue[T] = Queue.empty) extends Serializable {
  def add(element: T): FIFOBuffer[T] = this.copy(data = data.enqueue(element).take(capacity))
  def get: List[T] = data.toList
  def size: Int = data.size
}

//output (O)
case class WeatherEventAverage(stationId: String,
                               startTime: Timestamp,
                               endTime: Timestamp,
                               pressureAvg: Double,
                               tempAvg: Double)

// def mappingFunction(key: K, values: Iterator[I], state: GroupState[S]): O


object WeatherEventExample extends App {

  val spark = SparkSession
    .builder
    .appName("AbandonedCartsProcessor")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  case class Rate(timestamp: Timestamp, value: Long)

  val rate = spark
    .readStream
    .format("rate")
    .load()
    .as[Rate]

  //get randomStationIds
  val uids = List("d1e46a42", "d8e16e2a", "d1b06f88", "d2e710aa", "d2f731cc", "d4c162ee", "d4a11632", "d7e277b2", "d59018de", "d60779f6")

  def pickOne[T](list: List[T]): T = list(scala.util.Random.nextInt(list.size))

  val locationGenerator: () => (Double, Double) = {
    // Europe bounds
    val longBounds = (-10.89, 39.82)
    val latBounds = (35.52, 56.7)

    def pointInRange(bounds: (Double, Double)): Double = {
      val (a, b) = bounds
      Math.abs(scala.util.Random.nextDouble()) * b + a
    }

    () => (pointInRange(longBounds), pointInRange(latBounds))
  }

  val pressureGen: () => Double = () => scala.util.Random.nextDouble + 101.0
  val tempGen: () => Double = () => scala.util.Random.nextDouble * 60 - 20


  val weatherEvents: Dataset[WeatherEvent] = rate.map {
    case Rate(ts, value) => WeatherEvent(pickOne(uids), ts, locationGenerator(), pressureGen(), tempGen())
  }

  val weatherEventsMovingAverage = weatherEvents
    .withWatermark("timestamp", "2 minutes")
    .groupByKey(record => record.stationId)
    //    .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mappingFunction)
    .flatMapGroupsWithState(OutputMode.Update, GroupStateTimeout.ProcessingTimeTimeout)(flatMappingFunction)


  val query = weatherEventsMovingAverage
    .writeStream
    .format("console")
    .outputMode("update")
    .start()

  query.awaitTermination()

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // def mappingFunction(key: K, values: Iterator[I], state: GroupState[S]): O
  def mappingFunction(key: String, values: Iterator[WeatherEvent], state: GroupState[FIFOBuffer[WeatherEvent]]): WeatherEventAverage = {

    // get current state or create a new one if there's no previous state
    val currentState = state.getOption.getOrElse(
      new FIFOBuffer[WeatherEvent](10)
    )

    // enrich the state with the new events
    val updatedState = values.foldLeft(currentState) {
      case (st, ev) => st.add(ev)
    }

    // update the state with the enriched state
    state.update(updatedState)

    // if we have enough data, create a WeatherEventAverage from the accumulated state
    // otherwise, make a zeroed record
    val data = updatedState.get

    if (data.size > 2) {
      val start = data.head
      val end = data.last
      val pressureAvg = data.map(event => event.pressure).sum / data.size
      val tempAvg = data.map(event => event.temp).sum / data.size
      WeatherEventAverage(key, start.timestamp, end.timestamp, pressureAvg, tempAvg)
    } else {
      WeatherEventAverage(key, new Timestamp(0), new Timestamp(0), 0.0, 0.0)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // def flatMappingFunction(key: K, values: Iterator[I], state: GroupState[S]): Iterator[O]
  def flatMappingFunction(key: String, values: Iterator[WeatherEvent], state: GroupState[FIFOBuffer[WeatherEvent]]): Iterator[WeatherEventAverage] = {

    val ElementCountWindowSize = 10

    def stateToAverageEvent(key: String, data: FIFOBuffer[WeatherEvent]): Iterator[WeatherEventAverage] = {
      if (data.size == ElementCountWindowSize) {
        val events = data.get
        val start = events.head
        val end = events.last
        val pressureAvg = events.map(event => event.pressure).sum / data.size
        val tempAvg = events.map(event => event.temp).sum / data.size
        Iterator(WeatherEventAverage(key, start.timestamp, end.timestamp, pressureAvg, tempAvg))
      } else {
        Iterator.empty
      }
    }

    if (state.hasTimedOut) {
      // when the state has a timeout, the values are empty; this validation is only to illustrate that point
      assert(values.isEmpty, "When the state has a timeout, the values are empty")
      val result = stateToAverageEvent(key, state.get)
      // evict the timed-out state
      state.remove()
      // emit the result of transforming the current state into an output record
      result
    } else {
      // get current state or create a new one if there's no previous state
      val currentState = state.getOption.getOrElse(new FIFOBuffer[WeatherEvent](ElementCountWindowSize))

      // enrich the state with the new events
      val updatedState = values.foldLeft(currentState) { case (st, ev) => st.add(ev) }

      // update the state with the enriched state
      state.update(updatedState)

      state.setTimeoutDuration("30 seconds")

      // only when we have enough data, create a WeatherEventAverage from the accumulated state
      // before that, we return an empty result.
      stateToAverageEvent(key, updatedState)
    }
  }
}
