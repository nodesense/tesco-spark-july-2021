package workshop

import org.apache.spark.sql.SparkSession

case class Product(name : String, price: Double)

object S006_MovieLens extends  App {

  val spark: SparkSession  = SparkSession
    .builder()
    .master("local") // spark run inside hello world app
    //.master("spark://192.168.1.110:7077") // now driver runs the tasks on cluster
    .appName("MovieLens")
    .getOrCreate()

  // for converting DataFrame to Scala RDD
  import spark.implicits._

// TODO
//  val productRdd = sc.parallelize( List( Product("iphone", 100.0) ))
//  val productDf = productRdd.toDF()
//  productDf.printSchema()
//  productDf.show()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  // DataFrame is structured, iit has column, rows, data types
  // inferSchema is expensive, not meant for bigger files
  val movieDf = spark.read
                      .format("csv")
                      .option("header",  true)
                      .option("delimitter", ",")
                      .option("inferSchema",  true) // spark will scana all the records in the file, to determine data type
                      .load("/home/krish/ml-latest-small/movies.csv")


  println("partitions ", movieDf.rdd.getNumPartitions)

  movieDf.printSchema()
  movieDf.show(5)

  val movieSchema = movieDf.schema

  println(movieDf.rdd.take(5).toList )

  // convert movieDf to Rdd
  val movieRdd = movieDf.rdd
  println(movieRdd.take(4).toList)

  // convert rdd into DataFrame
  val movieDf2 = spark.createDataFrame(movieRdd, movieSchema)
  movieDf2.printSchema()
  println("Movies count ", movieDf2.count())
  // good practice to define custom schema

}
