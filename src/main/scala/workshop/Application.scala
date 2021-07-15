package workshop

import org.apache.spark.sql.SparkSession

object Application  {
  def DATA_DIR = "/home/krish/data"
  val META_DATA_URI = "thrift://172.20.0.110:9083"
  //val WAREHOUSE_URI =   "hdfs://krish.training.sh:9000/user/hive/warehouse"
  val WAREHOUSE_URI =   "s3a://stock/warehouse"

  def getPath(path: String) = DATA_DIR + "/" + path
  def getS3Path(bucket: String, path: String): String = "s3a://" + bucket + (if (path.startsWith("/")) path else  "/" + path)

  def getSparkConfig(appName: String) = {
    val spark: SparkSession  = SparkSession
      .builder()
      .master("local") // spark run inside hello world app
      .appName(appName)
      .config("hive.metastore.uris", Application.META_DATA_URI)
      .config("hive.metastore.warehouse.dir", Application.WAREHOUSE_URI)
      .enableHiveSupport()
      .getOrCreate()


//    val AccessKey = "Y9ANVPKK5K0MFZ2HRRO0"
//    val  SecretKey = "wVMgKFCxP7tCS7E+5KNkcBybjNrnG8um69pRoo44"


        val AccessKey = "minio"
        val  SecretKey = "minio123"


    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", AccessKey)
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", SecretKey)

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "localhost:9000")

    //  spark.sparkContext
    //    .hadoopConfiguration.set("spark.hadoop.fs.s3a.endpoint", "localhost:9000")

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.path.style.access", "true")

    spark.sparkContext.setLogLevel("WARN")

    spark
  }

}
