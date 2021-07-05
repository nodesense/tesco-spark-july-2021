name := "spark-workshop"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive

// libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.7" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.7"
