package workshop

import org.apache.spark.sql.{SaveMode, SparkSession}

object S015_SparkSQLHiveMetaThriftServer extends  App {
  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .config("hive.metastore.uris", "thrift://bigdata.training.sh:9083" )
    .config("hive.metastore.warehouse.dir", "hdfs://bigdata.training.sh:9000/user/hive/warehouse" )
    .appName("SparkDB")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  spark.sql("SHOW DATABASES").show()
  // show tables from default db
  spark.sql("SHOW TABLES").show()
   spark.sql("SELECT * FROM pokes").show()





}
