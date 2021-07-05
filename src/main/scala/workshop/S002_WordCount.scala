package workshop

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
// embed spark within JVM
// driver code, task scheduler etc runs with in JVM
object S002_WordCount extends  App {
  // entry point for DF, Streaming

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    .appName("WordCount")
    .getOrCreate()

  // RDD

  // spark context: the main component responsible for hodling all the RDDs, lineaage, DAG, DAG scheduler,
  // Spark context: create tasks, schedule the tasks, run the tasks across multiple workers
  // if task failed, reschedule task on another scheduler
  // co-ordinate/initiate shuffling...
  // every application should have 1 spark context
  // application is called as driver
  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN") // doesn't print INFO, DEBUG from now
  // read the data from text file
  // load the data into rdd
  // reading of a textfile, csv, all input files, jbdc etc, lazy execution
  // until any action method invoked, the file will not be loaded

  // Create RDD, lazy
  val textFile:RDD[String] = sc.textFile("input/textbook.txt")

  // transformation method, map, fileter, flatmap, groupby, reduceby etc
  // lazy functions, won't be executed until any action applied
  // building a lineage
  // remove the space around the line
  val strippedLinesRDD = textFile.map (line => line.trim())

  // we want to remove the extra empty lines
  // use filter , transformation, lazy function
  val nonEmptyLines = strippedLinesRDD.filter( line => !line.isEmpty)

  // split the line into word list
  val splitWords:RDD[Array[String]] = nonEmptyLines.map (line => line.split(" "))

  // flatten the word list into individual word
  val words = splitWords.flatMap( arr => arr)

  val nonEmptyWords = words.filter(word => !word.isEmpty)

  // wordPair/tuple Tuple2[String, Int] (scala, 1) (spark, 1)
  val wordPairs = nonEmptyWords.map (word => (word, 1))

  // acc is the accumulated word count , the value is 1 as we assigned in tuple
  val wordsCount = wordPairs.reduceByKey( (acc, value) => acc + value)

  // action method, eager method, execute, load the file, and print content
  wordsCount.foreach(println)

  // shall create a directory, and dump the outputs of partitions
  // if you running the program again, the delete the file manually
  wordsCount.saveAsTextFile("output/wordcounts")

 val lineCount = textFile.count()
 // println(lineCount)

}
