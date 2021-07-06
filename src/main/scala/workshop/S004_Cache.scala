package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object S004_Cache extends  App {

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    //.master("spark://192.168.1.110:7077") // now driver runs the tasks on cluster
    .appName("Cache")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  // creation of input rdd
  val rdd = sc.parallelize( 1 to 10)

  println("----------------")
  val rdd2 = rdd.filter ( n => {
    println("Filter called", n , n % 2 == 0)
    n % 2 == 0
  })

  println("----------------")
  val rdd3 = rdd2.map (n => {
    println("Map called", n , n * 10)
    n * 10
  })
  println("----------------")

  // Using rdd3 uncached one with action
  println ("MAX ", rdd3.max()) // runs through filter, map function first time
  println ("MIN ", rdd3.min()) // runs through filter, map function second time
  println ("SUM ", rdd3.sum()) // runs through filter, map function second time


  // cache, for gaining CPU
  // Spark has cache function, which can cache the RDD, DF in MEMORY, DISK, DISK_MEMORY etc
  // default, is MEMORY_DISK
  // persist and cache
  // caching shall be on worker node..
  // cache is lazy method, until is it used very first with action, result is not cached

  rdd3.cache() // this calls persist(MEMORY_AND_DISK) as option

  //rdd3.persist(StorageLevel.DISK_ONLY) // store the cache in Disk only

  println("Is cached RDD ", rdd3.cache())

  println("After cache")

  println ("MAX ", rdd3.max()) // runs through filter, map function first time, cache is stored
  println ("MIN ", rdd3.min()) // WILL NOT runs through filter,map, as the result is cached already
  println ("SUM ", rdd3.sum()) // WILL NOT runs through filter,map, as the result is cached already


  println("press enter to exit")
  scala.io.StdIn.readLine()
}
