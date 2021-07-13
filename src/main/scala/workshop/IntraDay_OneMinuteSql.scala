package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object IntraDay_OneMinuteSql extends  App {
  //val DataPath = "/home/krish/data/optimized-intraday/parquet"
  val DataPath = "/home/krish/data/optimized-intraday/parquet/Year=2021"


  val spark: SparkSession  = SparkSession
    .builder()
    .master("local[*]") // now driver runs the tasks on cluster
    .appName("StockMarketAnalytics")
    .config("spark.driver.cores", "1") // only in cluster mode
    .config("spark.executor.memory", "4g") // per executor
    .config("spark.executor.cores", "1")
  .getOrCreate()

  import spark.implicits._


  // we no need to use inferSchema
  val intraDayMinDfRaw = spark.read
    .format("parquet")
    .load(DataPath)

  intraDayMinDfRaw.printSchema();

  intraDayMinDfRaw.createOrReplaceTempView("intraday")
  /*

  SELECT pat_id,
        dept_id,
        ins_amt,
        First_value(ins_amt) OVER ( partition BY dept_id ORDER BY ins_amt ) AS low_ins_amt,
        Last_value(ins_amt) OVER ( partition BY dept_id ORDER BY ins_amt ) AS high_ins_amt
 FROM   patient;

   */

  /*

   |-- Open: double (nullable = true)
 |-- High: double (nullable = true)
 |-- Low: double (nullable = true)
 |-- Close: double (nullable = true)
 |-- Volume: long (nullable = true)
 |-- OI: long (nullable = true)
 |-- DateTime: timestamp (nullable = true)
 |-- Day: integer (nullable = true)
 |-- Ticker: string (nullable = true)
   */


//
//  spark.sql(
//    """
//      |SELECT Ticker,
//      |        Day,
//      |        first_value(Open) OVER ( partition BY Ticker, Day ORDER BY DateTime ) AS low_ins_amt,
//      |        last_value(Close) OVER ( partition BY Ticker, Day  ORDER BY DateTime Desc ) AS high_ins_amt
//      |        FROM intraday
//      |""".stripMargin).show(5)

  intraDayMinDfRaw.cache()
//
//  println("Intraday diff");
//  intraDayMinDfRaw.withColumn("DailyPoints", $"Close" - $"Open")
//    .show (5)


//
//  println("monthly low, high")
//  intraDayMinDfRaw.withColumn("DailyPoints", $"Close" - $"Open")
//    .groupBy("Ticker", "Month")
//    .agg(min("DailyPoints") as "MonthlyLow", max("DailyPoints") as "MonthlyHigh")
//    .orderBy("Ticker", "Month")
//    .show()

  val windowSpec  = Window.partitionBy("department").orderBy("salary")

 // val window = Window.partitionBy("Ticker", "Month").orderBy("DateTime").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  val window = Window.partitionBy("Ticker", "Year", "Month").orderBy("DateTime")
  val window2 = Window.partitionBy("Ticker", "Year", "Month").orderBy(desc("DateTime"))

  //
//  result = df.select(
//    "*",
//    first('ID').over(w_uf).alias("first_id"),
//  last('ID').over(w_uf).alias("last_id")
//  )
//
//  intraDayMinDfRaw.withColumn("DailyPoints", $"Close" - $"Open")
//    .select("Ticker", "Month", "DailyPoints")
//    .withColumn("first_value", first($"DailyPoints").over(window).alias("first_value"))
//    .withColumn("last_value", last($"DailyPoints").over(window).alias("last_value"))
//    .show(5)

//
//  intraDayMinDfRaw
//    .withColumn("OpenValue", first($"Open").over(window).alias("OpenValue"))
//    .withColumn("LastValue", last($"Close").over(window).alias("LastValue"))
//    .show(5)


//
//
//    intraDayMinDfRaw
//      .withColumn("OpenValue", first($"Open").over(window).alias("OpenValue"))
//      .withColumn("OpenDay", first($"DateTime").over(window).alias("OpenDay"))
//      .withColumn("LastValue", last($"Close").over(window).alias("LastValue"))
//      .withColumn("LastDay", last($"DateTime").over(window).alias("LastDay"))
//      .withColumn("row_number", row_number.over(window))
//      //.where($"row_number" === 1)
//      //.drop("row_number")
//      .orderBy("Ticker", "Month")
//      .show(100)

  intraDayMinDfRaw
    .withColumn("Year", year(col("DateTime")))
    .withColumn("OpenValue", first($"Open").over(window).alias("OpenValue"))
    .withColumn("OpenDay", first($"DateTime").over(window).alias("OpenDay"))
    .withColumn("LastValue", first($"Close").over(window2).alias("LastValue"))
    .withColumn("LastDay", first($"DateTime").over(window2).alias("LastDay"))
    .withColumn("row_number", row_number.over(window))
    .where($"row_number" === 1)
    .drop("row_number")
    .orderBy("Ticker", "DateTime")
    .show(100)

}
