package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import  org.apache.spark.sql.functions._

object S007_MovieLensAnalytics extends  App {
    val MoviesPath = "/home/krish/ml-latest-small/movies.csv"
    val RatingsPath = "/home/krish/ml-latest-small/ratings.csv"

    val spark: SparkSession  = SparkSession
      .builder()
      //.master("local") // spark run inside hello world app
      .master("spark://192.168.1.103:7077") // now driver runs the tasks on cluster
      .appName("MovieLensAnalytics")
      // .config("spark.cores.max", "48") // MAX CORE Across Cluster
      .config("spark.executor.memory", "4g")
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


  // examples
  // while appling transformation / lazy, spark create new dataframe
  // existing data frame is not changed, immutable
  // get new data frame with ratings >= 4
  val ratingsAbove4Df = ratingDf.filter ($"rating" >= 4)

  ratingsAbove4Df.show(10)

  // example select is for picking a specific column, distinct is for picking distinct of selected column
  // data is not sorted
  ratingDf.select("rating").distinct().show()

  // col is from  org.apache.spark.sql.functions._, it creates an object of Column
  // $"rating" is sugar of col("rating")
  ratingDf.select("rating").distinct().sort(col("rating")).show()

  // col("movieId"),
   ratingDf.sort(col("rating"),  col("timestamp")).show()

   movieDf.drop("genres").show(5)

   movieDf.select("movieId", "title").show(5)

  // derived columns, add new column, called NAME_UPPER, value will be in upper case
  movieDf.withColumn("NAME_UPPER", upper($"title")).show(10)

  // add constant columns
  movieDf.withColumn("imdb", lit(4) ).show(5)

  // concat to concat columns
  movieDf.withColumn("description",
                        concat(
                                col("title"),
                                lit (" is categorized as "),
                                col("genres") )
                              )
       .show(10)



  // get most popular movies
  // avg rating per movie id,
  // count number of users voted for that movie
  // and filter total ratings >= 100 and avg_rating > 3
  // avg("rating") creates a column named  avg(rating)
  // count("userId") creates a column named  count(userId)

  val popularMovies = ratingDf
                      .groupBy($"movieId")
                      .agg(avg("rating").alias("avg_rating"), count("userId"))
                      .withColumnRenamed("count(userId)", "total_rating")
                      .filter( ($"total_rating" >= 100 ) && ($"avg_rating" >= 3 ))
                      .sort(desc("avg_rating"))

  popularMovies.printSchema()
  popularMovies.show(200)


  // if your dataframe has 15 columns, you may use only 2 columns for joins
  val slimMovieDf = movieDf.select("movieId", "title")

  // the data is readily avaialbel in all the workers where executors running
  // all small tables, master/dimension tables can be broadcasted
  val broadcatedMovieDf = broadcast(slimMovieDf)

  val mostPopularMoviesList = popularMovies.join(broadcatedMovieDf, popularMovies("movieId") === movieDf("movieId"))
    .select(popularMovies("movieId"), $"title", $"avg_rating", $"total_rating" )

  mostPopularMoviesList.printSchema()
  mostPopularMoviesList.show()

  ratingDf.sort("userId", "rating", "movieId").select("userId", "rating", "movieId").show()

  println("press enter to exit")
  scala.io.StdIn.readLine()
}
