package workshop

import org.apache.spark.sql.SparkSession

object S005_Glom extends  App {

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    //.master("spark://192.168.1.110:7077") // now driver runs the tasks on cluster
    .appName("Cache")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  // creation of input rdd
  val rdd = sc.parallelize( 1 to 100)

  val rdd2 = rdd.repartition(4)

  // collect(), action method, collect data from    all paritions , bring back to driver
  // BAD Approach, performance, driver memory should be capable enough to handle all the load

  val data = rdd2.collect()
  println("All data ", data.toList)

  // take will pick the data from first partition onwards, if the first partitions has all requested numbers, it picks from first partition
  // good for sampling, print, interactive coding
  val data2 = rdd2.take(4)
  println("take data, ", data2.toList)

  // collect the data from each partition, doesn't merge
  // driver needs memory
  // NOT A GOOD for large data
  //array of array
  val data3 = rdd2.glom()
  println("glom data, ", data3)
  data3.foreach(list => println(list.toList))

  // iterate over all the values
  // action method
  rdd2.foreach( n => {
    print(" " + n)
  })

  println()


  // foreachparitions
  //used to custom process the data , like weriting the parittion dato destination which is not supported by spark

  // for each parition, the function is invoked, we can store the data to db, implement custom data sink
  rdd2.foreachPartition( partitionData => {
    // sorting here will use scala collection functions, not native spark
    // native spark -- cluster to sort
    // scala collection - runs on single jvm
    println("For each called")
    partitionData.foreach(println)  // print the numbers
  })

  println("press enter to exit")
  scala.io.StdIn.readLine()
}
