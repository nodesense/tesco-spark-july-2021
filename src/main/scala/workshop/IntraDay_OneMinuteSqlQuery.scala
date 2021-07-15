package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object IntraDay_OneMinuteSqlQuery extends  App {
  //val DataPath = "/home/krish/data/optimized-intraday/parquet"
  val DataPath = "/home/krish/data/optimized-intraday/parquet/Year=2018/Month=12/Day=03/Ticker=ACC"


  val spark: SparkSession  = SparkSession
    .builder()
    .master("local[*]") // now driver runs the tasks on cluster
    .appName("StockMarketAnalytics")
    .config("spark.driver.cores", "1") // only in cluster mode
    .config("spark.executor.memory", "4g") // per executor
    .config("spark.executor.cores", "1")
    .enableHiveSupport()
  .getOrCreate()

  import spark.implicits._
/*
  |-- Open: double (nullable = true)
  |-- High: double (nullable = true)
  |-- Low: double (nullable = true)
  |-- Close: double (nullable = true)
  |-- Volume: long (nullable = true)
  |-- OI: long (nullable = true)
  |-- DateTime: timestamp (nullable = true)
 */

 spark.sql(
   """
     CREATE EXTERNAL TABLE   intraday_par(
      Open Double,
       High Double,
       Low Double,
       Close Double,
       Volume BIGINT,
       OI BIGINT,
       DateTime Date
       )
PARTITIONED BY (Year Int, Month Int, Day Int, Ticker String)
STORED AS PARQUET
LOCATION '/home/krish/data/optimized-intraday/parquet'
     """)

  // we no need to use inferSchema
  val intraDayMinDfRaw = spark.read
    .format("parquet")
    .load(DataPath)

  intraDayMinDfRaw.printSchema();

  intraDayMinDfRaw.createOrReplaceTempView("intraday")

  // check simple count // it loads all the files content
  // spark.sql("SELECT COUNT(*) as TotalRecords from interday").show()

  // Query with partition
  spark.sql("SELECT COUNT(*) as TotalRecords from intraday").show()

  spark.sql("SELECT COUNT(*) as TotalRecords from intraday_par Where Year=2018 AND Month=12 AND Day=03  ").show()
}
