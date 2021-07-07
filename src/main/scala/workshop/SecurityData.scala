package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, DoubleType, LongType, TimestampType}

object SecurityData extends  App {

  // movie schema
  val SecuritySchema = StructType(
    List(
      StructField("Symbol", StringType, true), // true means nullable
      StructField("CompanyName", StringType, true),
      StructField("Series", StringType, true),
      StructField("ListingDate", TimestampType, true),
      StructField("PaidUpValue", DoubleType, true),
      StructField("MarketLot", LongType, true),
      StructField("ISIN", StringType, true),
      StructField("FaceValue", DoubleType, true)
    )
  )

  def getSecurities(spark: SparkSession, path: String) = {
    val securityDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("timestampFormat", "dd-MMM-yyyy")
      .schema(SecuritySchema)
      .load(path)

    securityDf
  }

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .appName("testapp")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val securitiesDF  = getSecurities(spark, Application.getPath("securities/"))

  securitiesDF.printSchema()

  securitiesDF.show(5)

  println(securitiesDF.count())

}
