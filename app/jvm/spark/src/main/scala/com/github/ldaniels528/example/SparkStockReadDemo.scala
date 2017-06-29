package com.github.ldaniels528.example

import com.databricks.spark.avro._
import org.apache.spark.sql.SparkSession

/**
  * Spark Stock Read Demo
  * @author lawrence.daniels@gmail.com
  */
object SparkStockReadDemo extends App {

  val spark = SparkSession.builder().master("local").getOrCreate()

  readParquet()

  def readAvro(): Unit = {
    val df = spark.read.avro("./tmp/A1498182522899/")
    df.createTempView("stocks")
    spark.sql("SELECT * FROM stocks WHERE ticker = 'AAPL' order by date desc").show(30)
  }

  def readAvro2JSON(): Unit = {
    val df = spark.read.avro("./tmp/A1498182522899/")
    df.createTempView("stocks")
    //df.toJSON.take(4).foreach(println)
    spark.sql("SELECT * FROM stocks WHERE ticker = 'AAPL' order by date desc").toJSON.take(30).foreach(println)
  }

  def readCSV(): Unit = {
    val df = spark.read.avro("./tmp/C1498182522899/")
    df.createTempView("stocks")
    spark.sql("SELECT * FROM stocks WHERE ticker = 'AAPL' order by date desc").show(30)
  }

  def readJSON(): Unit = {
    val df = spark.read.avro("./tmp/J1498182522899/")
    df.createTempView("stocks")
    spark.sql("SELECT * FROM stocks WHERE ticker = 'AAPL' order by date desc").show(30)
  }

  def readParquet(): Unit = {
    val df = spark.read.parquet("./tmp/P1498182522899/")
    df.createTempView("stocks")
    spark.sql("SELECT * FROM stocks WHERE ticker = 'AAPL' order by date desc").show(30)
  }


}
