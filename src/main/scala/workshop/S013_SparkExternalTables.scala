package workshop

import org.apache.spark.sql.SparkSession

object S013_SparkExternalTables extends  App {

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .config("spark.sql.warehouse.dir", "/home/krish/warehouse" )
    .appName("SparkDB")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  spark.sql("CREATE DATABASE IF NOT EXISTS sample").show()


  // cd ~
  // mkdir hdfs
   spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS sample.t3 (i int)  LOCATION '/home/krish/hdfs/t3'")
//
   spark.sql("INSERT INTO  sample.t3 VALUES(10)")
//
   spark.sql("SELECT * FROM sample.t3").show()

  spark.sql("DROP   TABLE  sample.t3 ")



}
