name := "spark-workshop"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"
val hadoopVersion = "3.0.0" // 3.1.0


// https://mvnrepository.com/artifact/org.apache.spark/spark-core

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive

// libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.7" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.7"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopVersion

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion


dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

//libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopVersion
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"
