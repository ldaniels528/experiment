package com.github.ldaniels528.example

import org.apache.spark.sql.SparkSession

/**
  * Spark Insert-Select Demo
  * @author lawrence.daniels@gmail.com
  */
object SparkInsertSelectDemo extends App {

  val spark = SparkSession.builder().master("local").getOrCreate()
  val dataFrame = spark.read
    .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
    .option("header", "true")
    .option("nullValue", "")
    .option("inferSchema", "true")
    .load("./companylist.csv")
  dataFrame.createTempView("companies")
  spark.sql("SELECT * FROM companies").show(5)

  dataFrame.write
      .json("./scratch/json")
}
