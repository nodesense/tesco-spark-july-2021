package workshop

// Spark general purpose data processing framework
// framework, it means, developer build the code , run on framework
// ETL, Analytics, ML, Streaming
// Scala, SQL,  R, Java, Python

// To run spark,
//  a cluster is needed, however it can be run in multiple mode
// Spark Runtime:
//  We need JVM 1.8 to run spark
//    run on standalone cluster, embedded mode, yarn cluster, mesos cluster

// embedded mode: A spark runtime embedded in our current project S001_HelloWorld

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// on the browser, http://localhost:4040
object S001_HelloWorld extends  App {
  //SQL, Dataframe
  val spark: SparkSession  = SparkSession
                             .builder()
                             .master("local") // spark run inside hello world app
                             .appName("HelloWorld")
                             .getOrCreate()
  // RDD
  val sc: SparkContext = spark.sparkContext

  // Resilient Distributed DataSet
  val rdd = sc.parallelize( 1 to 10)
  println("min", rdd.min())
  println("max", rdd.max())
  println("sum", rdd.sum())
  println("mean", rdd.mean())

  println("press enter to exit")
  scala.io.StdIn.readLine()
}
