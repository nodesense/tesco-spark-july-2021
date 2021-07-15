package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object S007_MovieLensAnalyticsExplain extends  App {
    val MoviesPath = "/home/krish/ml-latest-small/movies.csv"
    val RatingsPath = "/home/krish/ml-latest-small/ratings.csv"

    // task.maxFailures -- how many times the task can fails 3

    val spark: SparkSession  = SparkSession
      .builder()
      .master("local") // spark run inside hello world app
      //.master("spark://192.168.1.103:7077") // now driver runs the tasks on cluster
      .appName("MovieLensAnalytics")
      // .config("spark.cores.max", "48") // MAX CORE Across Cluster
      //.config("spark.executor.memory", "4g")
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


    // ratingDf.explain()
    //ratingDf.explain(true) // print physical plan, logical and optimized plans

  val popularMovies = ratingDf
    .groupBy($"movieId")
    .agg(avg("rating").alias("avg_rating"), count("userId"))
    .withColumnRenamed("count(userId)", "total_rating")
    .sort(desc("avg_rating")) // shall be performed after filter
    .filter( ($"total_rating" >= 100 ) && ($"avg_rating" >= 3 ))
    .coalesce(10)


  //movieDf.select(upper(col("title"))).explain(true)
  // Parsed Logical Plan - parse your code as is, includes DF/SQL, it won't resolve schema, column name exist or not
  // it won't check data types

  // Analyzed Logical Plan: data types, column exist or not, but won't optimize it

  // Optimized Logical Plan : optimized, like filter first then sort next

  // == Physical Plan == actual execution

 println("------")

  val mostPopularMoviesList = popularMovies.join(movieDf, popularMovies("movieId") === movieDf("movieId"))
    .select(popularMovies("movieId"), $"title", $"avg_rating", $"total_rating" )

 // mostPopularMoviesList.explain(true)


  // broadcast one

  println("==braodcast==")
  val mostPopularMoviesList2 = popularMovies.join(broadcast(movieDf), popularMovies("movieId") === movieDf("movieId"))
    .select(popularMovies("movieId"), $"title", $"avg_rating", $"total_rating" )

  // mostPopularMoviesList2.explain(true)
  mostPopularMoviesList2.show()


  println("press enter to exit")
  scala.io.StdIn.readLine()
}
