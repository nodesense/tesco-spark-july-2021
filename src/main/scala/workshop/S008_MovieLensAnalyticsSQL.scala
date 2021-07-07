package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import  org.apache.spark.sql.functions._
object S008_MovieLensAnalyticsSQL extends  App {
  val MoviesPath = "/home/krish/ml-latest-small/movies.csv"
  val RatingsPath = "/home/krish/ml-latest-small/ratings.csv"

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


  // SPARK SQL
     // Spark support SQL
     // Support databases, Hive compatible
     // each database can have tables [managed table], external tables [unmanaged tables], temp table/view

   // 1 . Temp table aka Temp view, In memory, not persisted
  //       Scope of temp table is with in Spark Driver/Application and with in Spark Session

  // Create a temp table/view using DataFrame
  // create a temp view/table, called movies in "default" db
  movieDf.createOrReplaceTempView("movies")
  ratingDf.createOrReplaceTempView("ratings")

  // use sql statements
  // execute sql and return DF
  // lazy statement
  val df1 = spark.sql("select * from ratings where rating >= 3") // lazy code
  df1.printSchema()
  // action method
  df1.show(10)

  spark.sql("select * from movies").show(5)
  spark.sql("select movieId, upper(title) from movies").show(5)

  val df2 = spark.sql("""
    SELECT movieId, avg(rating) AS avg_rating , count(rating) AS total_rating FROM ratings
    GROUP BY movieId
    HAVING total_rating >= 100 AND avg_rating >= 3
    ORDER BY avg_rating DESC
    """)

  df2.printSchema()
  df2.show(20)

  // how to create a temp view from sql statement

  spark.sql("""
    CREATE TEMP VIEW popular_movies AS
    SELECT movieId, avg(rating) AS avg_rating , count(rating) AS total_rating FROM ratings
    GROUP BY movieId
    HAVING total_rating >= 100 AND avg_rating >= 3
    ORDER BY avg_rating DESC
    """)


  spark.sql("select * from popular_movies").show(100)

  // how to we get teh data frame from temp view
  val popularMoviesDf = spark.table("popular_movies")
  popularMoviesDf.show(2)

  spark.sql(
    """
       CREATE TEMP VIEW most_popular_movies AS
       SELECT pm.movieId, title, avg_rating, total_rating  from popular_movies pm
       INNER JOIN movies on movies.movieId == pm.movieId
    """)

  spark.table("most_popular_movies").show()
}
