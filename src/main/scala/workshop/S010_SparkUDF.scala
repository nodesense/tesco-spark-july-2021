package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{udf, col}

object S010_SparkUDF extends  App {
  val MoviesPath = "/home/krish/ml-latest-small/movies.csv"
  val RatingsPath = "/home/krish/ml-latest-small/ratings.csv"

  // entry point for DF/SQL
  // an application/driver, can have 1 or more sessions
  // a session is an isolation, can have temp views, UDF - User Defined Function /scala/java/python,
  // not shared with other session on the same driver/application
  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    //.master("spark://192.168.1.110:7077") // now driver runs the tasks on cluster
    .appName("MovieLensAnalytics")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._
  // movie schema
  val MovieSchema = StructType(
    List(
      StructField("movieId", IntegerType, true), // true means nullable
      StructField("title", StringType, true),
      StructField("genres", StringType, true)
    )
  )

  val RatingSchema = StructType(
    List(
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("rating", DoubleType, true),
      StructField("timestamp", LongType, true)
    )
  )

  // we no need to use inferSchema
  val movieDf = spark.read
    .format("csv")
    .option("header",  true)
    .option("delimitter", ",")
    .schema(MovieSchema) // use the Schema
    .load(MoviesPath)

  // we no need to use inferSchema
  val ratingDf = spark.read
    .format("csv")
    .option("header",  true)
    .option("delimitter", ",")
    .schema(RatingSchema) // use the Schema
    .load(RatingsPath)

  // User Defined functions
  // upper, sort, lower, abs, sin, tan etc are built in functions which can be used in SQL
  // you need custom transformation/handling a column data, how to do that?
  // UDF - User Defined Functions, add custom functions that can be used in sql

  val square = (s: Long) => s * s

  // register spark UDF on session
  // UDF is for local session
  // the square function code is moveed to worker before running the tasks
  spark.udf.register("square", square)
  ratingDf.createOrReplaceTempView("ratings")
  // the column would be UDF:square(cast(rating as bigint))
  spark.sql("select movieId, rating, square(rating) from ratings").show(20)

  // COLUMN ALIAS
  spark.sql("select movieId, rating, square(rating) as rating2 from ratings").show(20)

  // Using UDF with data frame

    // enclose the function within udf (the udf is act like higher order function)
  val square_udf = udf(square)

  ratingDf.select(square_udf(col("rating")).as("squared")).show(5)
  ratingDf.select(square_udf(col("rating")) as "squared").show(5)

}
