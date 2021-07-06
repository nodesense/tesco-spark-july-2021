package workshop

import org.apache.spark.sql.SparkSession

// Driver
object S003_Partitions extends  App {

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    //.master("spark://192.168.1.110:7077") // now driver runs the tasks on cluster
    .appName("WordCount")
    .getOrCreate()

  val sc = spark.sparkContext

  // creation of input rdd
  val rdd = sc.parallelize( 1 to 100)
  println("Partitions ", rdd.getNumPartitions)

  // split the data into 4 partitions, 4 tasks
  val rdd2 = rdd.repartition(4)

  // rdd lineage
  val processedRdd = rdd2.filter( n => n % 2 == 0)
                        .map(n => n * 10)

  println("processedRdd Partitions ", rdd.getNumPartitions)
  // check browser http://localhost:4040    watch for right port number

  // action method, create job, stages, tasks, use workers and get the job done
  val result = processedRdd.min () // 20

  println("result ", result)

  // create new job, independent of min we performed before
  // then create stages inside job
  // then create tasks in stages
  // run the runs and partitions all over again
  // create partitions etc..
  val max = processedRdd.max() // invoking action, we are reusing Rdd

  // Not for production, only to demo, each action method invoke a job..
  for (i <- 1 to 100) {
    val sum = processedRdd.sum()
    println("sum", sum) // 100 jobs, 2 stages each, 4 tasks per stage x 400 tasks + 1
  }
  println("press enter to exit")
   scala.io.StdIn.readLine()
}
