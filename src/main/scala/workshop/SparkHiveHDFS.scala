package workshop

import org.apache.spark.sql.SparkSession

object SparkHiveHDFS extends  App {
  // val HOST = "135.181.203.55"
  val HOST = "192.168.1.103"


  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .config("hive.metastore.uris", s"thrift://$HOST:9083")
    .config("hive.metastore.warehouse.dir", "hdfs://$HOST:8020/user/hive/warehouse")
    .appName("testapp")
    .enableHiveSupport() // don't forget to enable hive support
    .getOrCreate()


   spark.sql("show databases").show()
  spark.sql("show tables").show()
   spark.sql("select * from pokes").show()

}
