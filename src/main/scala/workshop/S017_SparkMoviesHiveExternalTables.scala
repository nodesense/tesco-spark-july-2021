package workshop

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import  org.apache.spark.sql.functions._

object S017_SparkMoviesHiveExternalTables extends  App {

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .config("hive.metastore.uris", "thrift://bigdata.training.sh:9083" ) // consist of meta data server
    .config("hive.metastore.warehouse.dir", "hdfs://bigdata.training.sh:8020/user/hive/warehouse" ) // for the hive data location
     // NOT NEEDED when using "hive.metastore.warehouse.dir"
   // .config("spark.sql.warehouse.dir", "hdfs://bigdata.training.sh:8020/user/hive/warehouse" )
    .appName("SparkDB")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // CREATE A META DATA RECORD in HIVE META SERVER  thrift://bigdata.training.sh:9083
  // A DATABASE in HIVE META SERVER
  spark.sql("CREATE DATABASE IF NOT EXISTS moviedb2").show()

  // CREATE A META TABLE [External Table] movies UNDER META DATABASE  moviedb2 IN HIVE META SERVER  thrift://bigdata.training.sh:9083
  // External Table
  //   The meta data now is managed by Meta Data Server
  //   the is not managed by HIVE since it is external
  //   moviedb2.movies table refering to a file/directory located in HDFS  hdfs://bigdata.training.sh:8020/user/movieset/movies
  //      where as movies is CSV file, delimited by , ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  spark.sql(
    """
      CREATE EXTERNAL TABLE if NOT EXISTS moviedb2.movies (
          movieId INT,
          title  STRING,
          genres  STRING
      )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION 'hdfs://bigdata.training.sh:8020/user/movieset/movies'
        TBLPROPERTIES ("skip.header.line.count"="1")
      """)

  // WHO IS RUNNING THIS QUERY?
  // Spark has Sql Analyser, reads and parse the SQL statement
  // it sees a db name moviedb2, table name movies, it has no idea what columns, data types etc, no idea where data located
  // spark send request to hive meta data store to get META DATA about db and table both
  // meta data server responds with definition of database, table meta data [column, data types], then the hadoop location where data stored
  //  Spark will send request to name node hdfs://bigdata.training.sh:8020, try get the location / blocks information of the data
  // Name node shall check the permissions / acl, if all ok, it sends back information about blocks, data node where data is stored
  // The spark send request to datanode 1 and datanode 2, for requesting blocks
  // spark create partitions, move the data to partitions for distributed processing
  spark.sql("SELECT * FROM moviedb2.movies").show(5)
  spark.sql("SELECT count(*) FROM moviedb2.movies").show()

// 9743 - previous count

  spark.sql(
    """
      CREATE EXTERNAL TABLE if NOT EXISTS moviedb2.ratings (
          userId INT,
          movieId  INT,
          rating  Double,
          timestamp  BIGINT
      )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION 'hdfs://bigdata.training.sh:8020/user/movieset/ratings'
        TBLPROPERTIES ("skip.header.line.count"="1")
      """)

  spark.sql("SELECT * FROM moviedb2.ratings").show(5)
  spark.sql("SELECT rating, CAST(rating as BIGINT) FROM moviedb2.ratings").show(5)


  // query using spark query engine
  val movieDf2 = spark.sql("select * from moviedb2.movies")


  // Write the data frame as parquet format,  90% less size
  movieDf2.write.mode("overwrite").parquet("hdfs://bigdata.training.sh:8020/user/krish/moviedb/movies-parquet")

  // reading parquet file using Spark

  val MovieSchema = StructType(
    List(
       StructField("movieId", IntegerType, true),
      StructField("rating", DoubleType, true),
      StructField("timestamp", LongType, true)
    )
  )


  // parquet read using spark
  // schema for parquet can be build from meta data within parquet without scanning the content
  val movieDf = spark.read
    .format("parquet")
    .load("hdfs://bigdata.training.sh:8020/user/krish/moviedb/movies-parquet")

  println("parquet data")
  movieDf.printSchema()
  movieDf.show(5)

  // Create external table for parquet format...


  spark.sql(
    """
      CREATE EXTERNAL TABLE if NOT EXISTS moviedb2.movies_parquet (
          movieId INT,
          title  STRING,
          genres  STRING
      )
        STORED AS PARQUET
        LOCATION 'hdfs://bigdata.training.sh:8020/user/krish/moviedb/movies-parquet'
      """)

  println("Data from parquet external table")
  // Using Parquet to read the data, column storage, effieient,
  spark.sql("SELECT * FROM moviedb2.movies_parquet ");

}
