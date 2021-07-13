package workshop

import org.apache.spark.sql.SparkSession

object S016_Spark_Hive_Database extends  App {

  // Spark DB uses Hive Meta data
  // Hive Meta is information about db
  //    1. Database name, tables, table names, columns [name, datatypes] and where exactlt the content is located

  // 3 types of tables
  // 1. managed table - meta data, data is managed by spark. Use DF, SQL to insert/read/write data
                       // if we drop the table, the folder/data will be deleted
  // 2. external table - meta data is managed by spark, the content is stored in data lake [hdfs, file system, s3, adls],
                                                       // the content is managed by ETL application
                      // if we drop external table, only meta data is deleted

  // 3. temp table/view, global temp view/table

  // on your home directory, create a folder called warehouse

  // cd ~
  // mkdir warehouse

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .config("hive.metastore.uris", "thrift://bigdata.training.sh:9083" ) // consist of meta data
    .config("hive.metastore.warehouse.dir", "hdfs://bigdata.training.sh:8020/user/hive/warehouse" ) // for the hive data location
    //.config("spark.sql.warehouse.dir", "hdfs://bigdata.training.sh:8020/user/hive/warehouse" )
    .appName("SparkDB2")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  spark.sql("show databases").show()

  spark.sql("CREATE DATABASE IF NOT EXISTS krishdb100").show()

  spark.sql("show databases").show()

  spark.sql("CREATE TABLE IF NOT EXISTS krishdb100.src (key INT, value STRING)")
  // managed table
  spark.sql("INSERT INTO TABLE krishdb100.src values (1, 'ONE')")
  spark.sql("SELECT * FROM krishdb100.src").show()

  // spark.sql("DROP TABLE  IF EXISTS krishdb100.src")
}
