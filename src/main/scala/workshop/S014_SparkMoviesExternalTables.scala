package workshop
import org.apache.spark.sql.SparkSession

object S014_SparkMoviesExternalTables extends  App {

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .config("spark.sql.warehouse.dir", "/home/krish/warehouse" )
    .appName("SparkDB")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  spark.sql("CREATE DATABASE IF NOT EXISTS moviedb").show()

  spark.sql(
    """
      CREATE EXTERNAL TABLE if NOT EXISTS moviedb.movies (
          movieId INT,
          title  STRING,
          genres  STRING
      )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION '/home/krish/hdfs/movies'
        TBLPROPERTIES ("skip.header.line.count"="1")
      """)

  spark.sql("SELECT * FROM moviedb.movies").show(5)



  spark.sql(
    """
      CREATE EXTERNAL TABLE if NOT EXISTS moviedb.ratings (
          userId INT,
          movieId  INT,
          rating  Double,
          timestamp  BIGINT
      )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION '/home/krish/hdfs/ratings'
        TBLPROPERTIES ("skip.header.line.count"="1")
      """)

  spark.sql("SELECT * FROM moviedb.ratings").show(5)


}
