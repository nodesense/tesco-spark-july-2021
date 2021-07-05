package workshop

import org.apache.spark.sql.SparkSession

object SparkHiveHDFS extends  App {
  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .config("hive.metastore.uris", "thrift://135.181.203.55:9083")
    .config("hive.metastore.warehouse.dir", "hdfs://135.181.203.55/:8020/user/hive/warehouse")
    .appName("testapp")
    .enableHiveSupport() // don't forget to enable hive support
    .getOrCreate()


   spark.sql("show databases").show()
  spark.sql("show tables").show()
   spark.sql("select * from pokes").show()

}
