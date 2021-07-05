package workshop

import org.apache.spark.sql.SparkSession

// Driver
object S003_Partitions extends  App {

  val spark: SparkSession  = SparkSession
    .builder()
    //.master("local") // spark run inside hello world app
    .master("spark://192.168.1.110:7077") // now driver runs the tasks on cluster
    .appName("WordCount")
    .getOrCreate()

  val sc = spark.sparkContext

  // creation of input rdd
  val rdd = sc.parallelize( 1 to 100)
  println("Partitions ", rdd.getNumPartitions)

  // split the data into 2 partitions, 2 tasks
  val rdd2 = rdd.repartition(2)

  // rdd lineage
  val processedRdd = rdd2.filter( n => n % 2 == 0)
                        .map(n => n * 10)

  println("processedRdd Partitions ", rdd.getNumPartitions)
  // check browser http://localhost:4040    watch for right port number

  // action method, create job, stages, tasks, use workers and get the job done
  val result = processedRdd.min () // 20

  println("result ", result)

  println("press enter to exit")
  // scala.io.StdIn.readLine()
}
