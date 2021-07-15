package workshop


import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import  org.apache.spark.sql.functions._

object ParquetAWSExample extends App{



  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", "minio")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "minio123")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
  spark.sparkContext
    .hadoopConfiguration.set("spark.hadoop.fs.s3a.endpoint", "localhost:9000")

  spark.sparkContext
    .hadoopConfiguration.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

  spark.sparkContext
    .hadoopConfiguration.set("spark.hadoop.fs.s3a.path.style.access", "true")


  val RatingSchema = StructType(
    List(
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("rating", DoubleType, true),
      StructField("timestamp", LongType, true)
    )
  )

  spark
    .read
    .schema(RatingSchema)
    .format("minioSelectCSV") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet

  .load("s3://path/to/my/datafiles")


  // spark.sparkContext
  // .hadoopConfiguration.set("fs.s3a.path.style.access", "true")

  val data = Seq(("James ","Rose","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","Mary","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","1234","F",-1)
  )

  val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
  import spark.sqlContext.implicits._
  val df = data.toDF(columns:_*)

  df.show()
  df.printSchema()

  df.write
    .parquet("s3a://sparkbyexamples/parquet/people.parquet")


  val parqDF = spark.read.parquet("s3a://sparkbyexamples/parquet/people.parquet")
  parqDF.createOrReplaceTempView("ParquetTable")

  spark.sql("select * from ParquetTable where salary >= 4000").explain()
  val parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")

  parkSQL.show()
  parkSQL.printSchema()

  df.write
    .partitionBy("gender","salary")
    .parquet("s3a://sparkbyexamples/parquet/people2.parquet")

  //    val parqDF2 = spark.read.parquet("s3a://sparkbyexamples/parquet/people2.parquet")
  //    parqDF2.createOrReplaceTempView("ParquetTable2")
  //
  //    val df3 = spark.sql("select * from ParquetTable2  where gender='M' and salary >= 4000")
  //    df3.explain()
  //    df3.printSchema()
  //    df3.show()
  //
  //    val parqDF3 = spark.read
  //      .parquet("s3a://sparkbyexamples/parquet/people.parquet/gender=M")
  //    parqDF3.show()


}