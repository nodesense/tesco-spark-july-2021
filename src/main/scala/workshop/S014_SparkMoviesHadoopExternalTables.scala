package workshop

import org.apache.spark.sql.SparkSession

object S014_SparkMoviesHadoopExternalTables extends  App {

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .config("hive.metastore.uris", "thrift://bigdata.training.sh:9083" )
    .config("hive.metastore.warehouse.dir", "hdfs://bigdata.training.sh:9000/user/hive/warehouse" )
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
        LOCATION 'hdfs://bigdata.training.sh:8020/user/movieset/movies'
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
        LOCATION 'hdfs://bigdata.training.sh:8020/user/movieset/ratings'
        TBLPROPERTIES ("skip.header.line.count"="1")
      """)

  spark.sql("SELECT * FROM moviedb.ratings").show(5)


}
