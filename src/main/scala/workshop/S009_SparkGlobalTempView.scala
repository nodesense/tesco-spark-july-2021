package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object S009_SparkGlobalTempView extends  App {
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

  // this temp view ratings is crated inside spark session
  ratingDf.createOrReplaceTempView("ratings")
  spark.sql("select * from ratings").show(5)

  // create global_temp views
  // able to create a view as global temp view which can be availabel across multiple session within spark application
  movieDf.createOrReplaceGlobalTempView("movies")

  // Note the db name global_temp in front the table
  spark.sql("select * from global_temp.movies").show(5)

  // we can create new session using newSession
  // session2
  val spark2 = spark.newSession()
  // THIS WILL NOT WORK
  // spark2 is different session, it cannot access temp views, udf from spark session
  // Table or view 'ratings' not found in database 'default';
  // spark2.sql("select * from ratings").show(5)

  // WORKS
  // spark2 can access global_temp.movies as it is global temp table
  spark2.sql("select * from global_temp.movies").show(5)
}
