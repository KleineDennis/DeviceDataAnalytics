package other.example

import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession


object HudiExample extends App {

  val spark = SparkSession
    .builder()
    .appName("HudiExample")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  import spark.implicits._

  val tableName = "hudi_cow_table"
  val basePath = "/Users/denniskleine/workspace/DeviceDataAnalytics/src/main/resources/hudi_cow_table"
  val dataGen = new DataGenerator

  val inserts = convertToStringList(dataGen.generateInserts(10)).asScala.toList
  val df = spark.read.json(inserts.toDS)
  df.write.format("org.apache.hudi")
    .options(getQuickstartWriteConfigs)
    .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
    .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
    .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
    .option(TABLE_NAME, tableName)
    .mode(Overwrite)
    .save(basePath)

  //roViewDF
  spark.read
    .format("org.apache.hudi")
    .load(basePath + "/*/*/*/*")
    .createOrReplaceTempView("hudi_ro_table")
  spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_ro_table where fare > 20.0").show()
  spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_ro_table").show()

  val updates = convertToStringList(dataGen.generateUpdates(10)).asScala.toList
  val df2 = spark.read.json(updates.toDS)
  df2.write.format("org.apache.hudi")
    .options(getQuickstartWriteConfigs)
    .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
    .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
    .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
    .option(TABLE_NAME, tableName)
    .mode(Append)
    .save(basePath)

  spark.read
    .format("org.apache.hudi")
    .load(basePath + "/*/*/*/*")
    .createOrReplaceTempView("hudi_ro_table")
  spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_ro_table where fare > 20.0").show()
  spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_ro_table").show()

  val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_ro_table order by commitTime").map(k => k.getString(0)).take(50)
  val beginTime = commits(commits.length - 2) // commit time we are interested in

  // incrementally query data
  spark.read
    .format("org.apache.hudi")
    .option(VIEW_TYPE_OPT_KEY, VIEW_TYPE_INCREMENTAL_OPT_VAL)
    .option(BEGIN_INSTANTTIME_OPT_KEY, beginTime)
    .load(basePath)
    .createOrReplaceTempView("hudi_incr_table")
  spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incr_table where fare > 20.0").show()

  val endTime = commits(commits.length - 2) // commit time we are interested in

  //incrementally query data
  spark.read
    .format("org.apache.hudi")
    .option(VIEW_TYPE_OPT_KEY, VIEW_TYPE_INCREMENTAL_OPT_VAL)
    .option(BEGIN_INSTANTTIME_OPT_KEY, "000")
    .option(END_INSTANTTIME_OPT_KEY, endTime)
    .load(basePath)
    .createOrReplaceTempView("hudi_incr_table")
  spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incr_table where fare > 20.0").show()

  spark.stop()
}
