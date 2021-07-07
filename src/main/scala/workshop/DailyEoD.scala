package workshop

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DailyEoD extends  App {

  //MKT,SERIES,SYMBOL,SECURITY,PREV_CL_PR,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,NET_TRDVAL,NET_TRDQTY,IND_SEC,CORP_IND,TRADES,HI_52_WK,LO_52_WK
 // Y, , ,Nifty 50,     15834.35,     15813.75,     15914.20,     15801.00,     15818.25,677495563988.08,3564283906,Y, ,23269988,     15915.65,     10562.90


  val DailySchema = StructType(
    List(
      StructField("MKT", StringType, true),
      StructField("SERIES", StringType, true),
      StructField("SYMBOL", StringType, true),
      StructField("SECURITY", StringType, true),
      StructField("PREV_CL_PR", DoubleType, true),
      StructField("OPEN_PRICE", DoubleType, true),
      StructField("HIGH_PRICE", DoubleType, true),
      StructField("LOW_PRICE", DoubleType, true),
      StructField("CLOSE_PRICE", DoubleType, true),
      StructField("NET_TRDVAL", DoubleType, true),
      StructField("NET_TRDQTY", DoubleType, true),
      StructField("IND_SEC", StringType, true),
      StructField("CORP_IND", StringType, true),
      StructField("TRADES", DoubleType, true),
      StructField("HI_52_WK", DoubleType, true),
      StructField("LO_52_WK", DoubleType, true)
    )
  )

  def getDailyEoD(spark: SparkSession, path: String) = {
    val dailyDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .schema(DailySchema)
      .load(path)

    dailyDf
  }


  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .appName("testapp")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  import spark.implicits._



}
