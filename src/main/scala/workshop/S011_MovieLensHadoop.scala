package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object S011_MovieLensHadoop extends  App {
  // Create a folder  movieset under users in HDFS usign hue UI
  // Upload movielens data csv files into movieset folder using Hue

  val MoviesPath = "hdfs://135.181.203.55:8020/user/movieset/movies.csv"
  val RatingsPath = "hdfs://135.181.203.55:8020/user/movieset/ratings.csv"

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    //.master("spark://192.168.1.110:7077") // now driver runs the tasks on cluster
    .appName("MovieLensAnalytics")
    .getOrCreate()

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

  movieDf.printSchema()
  ratingDf.printSchema()

  movieDf.show(5)
  ratingDf.show(5)

  val popularMovies = ratingDf
    .groupBy($"movieId")
    .agg(avg("rating").alias("avg_rating"), count("userId"))
    .withColumnRenamed("count(userId)", "total_rating")
    .filter( ($"total_rating" >= 100 ) && ($"avg_rating" >= 3 ))
    .sort(desc("avg_rating"))

  popularMovies.printSchema()
  popularMovies.show(200)

  val mostPopularMoviesList = popularMovies.join(movieDf, popularMovies("movieId") === movieDf("movieId"))
    .select(popularMovies("movieId"), $"title", $"avg_rating", $"total_rating" )

  mostPopularMoviesList.printSchema()
  mostPopularMoviesList.show()

  // TODO: Write to HDFS


}
