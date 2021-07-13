package workshop

import org.apache.spark.sql.{SaveMode, SparkSession}

object S014_SparkSQLHiveMetaServer extends  App {
  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .config("hive.metastore.uris", "thrift://bigdata.training.sh:9083" )
    .config("hive.metastore.warehouse.dir", "hdfs://bigdata.training.sh:8020/user/hive/warehouse" )
    .appName("SparkDB")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  spark.sql("SHOW DATABASES").show()
  // show tables from default db
  spark.sql("SHOW TABLES").show()


  spark.sql("SHOW TABLES IN moviedb").show()


  spark.sql("SHOW TABLES IN default").show()



  spark.sql("""
    CREATE TEMP VIEW popular_movies AS
    SELECT movieId, avg(rating) AS avg_rating , count(rating) AS total_rating FROM moviedb.ratings
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
       INNER JOIN moviedb.movies mov on mov.movieId == pm.movieId
    """)

  // table to expose table as data frame
  val mostPopularMovies = spark.table("most_popular_movies")
  mostPopularMovies.show()

  //Write to location
  // Write to database as table
  // writting to external table
  // if table not found, spark will create table automatically
  // by default it uses parquet format, we can also use csv/json etc
  mostPopularMovies.write.mode(SaveMode.Overwrite).saveAsTable("moviedb.most_popular_movies")

  // query result back from db
  spark.sql("SELECT * FROM moviedb.most_popular_movies").show()
}
