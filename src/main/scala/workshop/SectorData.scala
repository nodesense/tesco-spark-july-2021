package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import workshop.SectorData.spark

object SectorData extends  App {

  // movie schema
  val SectorSchema = StructType(
    List(
      StructField("CompanyName", StringType, true), // true means nullable
      StructField("Industry", StringType, true),
      StructField("Symbol", StringType, true),
      StructField("Series", StringType, true),
      StructField("ISIN", StringType, true)
    )
  )

  def getSectorIndexes(spark: SparkSession, path: String) = {
    val sectorIndex = spark.read
      .format("csv")
      .option("header",  true)
      .option("delimitter", ",")
      .schema(SectorSchema)
      .load(path)

    sectorIndex
  }

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .appName("testapp")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val sectorIndexDF  = getSectorIndexes(spark, Application.getPath("sectors/"))

  sectorIndexDF.printSchema()

  sectorIndexDF.show(5)

  println(sectorIndexDF.count())



}
