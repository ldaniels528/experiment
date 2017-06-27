package com.github.ldaniels528.example

import com.databricks.spark.avro._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * Spark Stock Write Demo
  * @author lawrence.daniels@gmail.com
  */
object SparkStockWriteDemo extends App {

  val spark = SparkSession.builder().master("local").getOrCreate()
  val rdd = spark.sparkContext.textFile("./data/*/*.txt") /*.repartition(4)*/ flatMap {
    case line if line.trim.isEmpty | line.contains("<") => None
    case line =>
      line.split("[,]").toList match {
        case ticker :: date :: open :: high :: low :: close :: vol :: Nil =>
          val year = date.take(4).toInt
          val month = date.slice(4, 6).toInt
          Some((year, month, ticker, date.toLong, open.toDouble, high.toDouble, low.toDouble, close.toDouble, vol.toLong))
        case _ => None
      }
  }

  // create the data frame
  val df = spark.createDataFrame(rdd).toDF("year", "month", "ticker", "date", "open", "high", "low", "close", "vol")

  val parameters = Map("recordName" -> "StockData", "recordNamespace" -> "com.databricks.spark.avro")
  val now = System.currentTimeMillis()

  writeAvro(df)
  writeCSV(df)
  writeJSON(df)
  writePaquet(df)

  def writeAvro(df: sql.DataFrame): Unit = {
    df.write
      .options(parameters)
      .partitionBy("year", "month")
      .avro(s"./tmp/A$now")
  }

  def writeCSV(df: sql.DataFrame): Unit = {
    df.write
      .partitionBy("year", "month")
      .csv(s"./tmp/C$now")
  }

  def writeJSON(df: sql.DataFrame): Unit = {
    df.write
      .partitionBy("year", "month")
      .json(s"./tmp/J$now")
  }

  def writePaquet(df: sql.DataFrame): Unit = {
    df.write
      .partitionBy("year", "month")
      .parquet(s"./tmp/P$now")
  }

}
