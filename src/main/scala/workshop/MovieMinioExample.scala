package workshop

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object MovieMinioExample extends  App {
  val spark = Application.getSparkConfig("Test")
  import spark.implicits._

  val RatingSchema = StructType(
    List(
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("rating", DoubleType, true),
      StructField("timestamp", LongType, true)
    )
  )

  spark
    .read
    .schema(RatingSchema)
    .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
    .load("s3a://movieset/ratings/ratings.csv")
    .show()


  spark.sql(
    """
     SHOW DATABASES
      """).show()

  spark.sql(
    """
      SELECT * FROM  foo
      """).show()

}
