package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, TimestampType,  StringType, StructField, StructType}

import  org.apache.spark.sql.functions._
object IntraDay_OneMinute extends  App {
  val DataPath = "/home/krish/data/intraday/**/*.txt"
  // val DataPath = "/home/krish/data/intraday/IntradayData_AUG2020/*.txt"

//  --num-executors` (or `spark.executor.instances`)
//
//  spark.dynamicAllocation.initialExecutors
//  spark.dynamicAllocation.minExecutors
//  spark.dynamicAllocation.maxExecutors
//
  val spark: SparkSession  = SparkSession
    .builder()
    //.master("local[*]") // spark run inside hello world app
    //.master("spark://192.168.1.110:7077") // now driver runs the tasks on cluster
    .master("spark://192.168.1.103:7077") // now driver runs the tasks on cluster
    .appName("StockMarketAnalytics")
    .config("spark.driver.cores", "1") // only in cluster mode
    .config("spark.executor.memory", "4g") // per executor
  //  .config("spark.executor.instances", "4") //
  .config("spark.dynamicAllocation.enabled", "true")
  .config("spark.shuffle.service.enabled", "true") // must be true for spark.dynamicAllocation.enabled = true

    .config("spark.dynamicAllocation.initialExecutors", "1") //
  .config("spark.dynamicAllocation.minExecutors", "2") //
  .config("spark.dynamicAllocation.maxExecutors", "4") //
  .config("spark.cores.max", "4")
  //  spark.dynamicAllocation.initialExecutors
  //  spark.dynamicAllocation.minExecutors
  //  spark.dynamicAllocation.maxExecutors

    .config("spark.executor.cores", "1")

  // Not good
    //.config("spark.network.timeout", "10000000")
   // .config("spark.executor.heartbeatInterval", "10000000")


  .getOrCreate()

  import spark.implicits._

  // OI - Open Interest

  // Ticker,Date,Time,Open,High,Low,Close,Volume,OI
// Aug data
  //ADANIENT,20200803,09:16,175.15,175.55,171.20,174.75,31634,0

  val StockSchema = StructType(
    List(
      StructField("Ticker", StringType, true),
      StructField("Date", StringType, true), // TimestampType with , yyyyMMdd
      StructField("Time", StringType, true),
      StructField("Open", DoubleType, true),
      StructField("High", DoubleType, true),
      StructField("Low", DoubleType, true),
      StructField("Close", DoubleType, true),
      StructField("Volume", LongType, true),
      StructField("OI", LongType, true)
    )
  )

  // we no need to use inferSchema
  val intraDayMinDfRaw = spark.read
    .format("csv")
    .option("header",  false)
    .option("delimitter", ",")
    .option("timestampFormat", "yyyyMMdd") //20200803
    .schema(StockSchema) // use the Schema
    .load(DataPath)

  intraDayMinDfRaw.show(5)

  // intraDayMinDf.select(date_format(col("Time"), "h:m")).show(5)
  val intraDayMinDf = intraDayMinDfRaw.withColumn("DateTimeStr", concat( col("Date"), lit(" "), col("Time")))
    .withColumn("DateTime", to_timestamp(col("DateTimeStr"), "yyyyMMdd hh:mm" ))
    .withColumn("Date", to_timestamp(col("Date"), "yyyyMMdd" ))
    .drop($"DateTimeStr")
    .drop($"Time")
    .drop($"Date")

  intraDayMinDf.show(5)
  intraDayMinDf.withColumn("Year", date_format(col("DateTime"), "yyyy"))
              .withColumn("Month", date_format(col("DateTime"), "MM"))
              .withColumn("Day", date_format(col("DateTime"), "dd"))
              .drop($"__HIVE_DEFAULT_PARTITION__")
                .write
                .partitionBy("Year", "Month", "Day", "Ticker")
                .format("parquet")
                .mode("overwrite")
                .save("/home/krish/data/optimized-intraday/parquet")


  // select something from intrday_parquet where Year=2018 and month=12 and day=03 and Ticker=ACC
  println("press enter to exit")
  scala.io.StdIn.readLine()
}
