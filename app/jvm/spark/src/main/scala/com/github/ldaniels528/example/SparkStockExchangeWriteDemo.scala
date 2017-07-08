package com.github.ldaniels528.example

import java.io.File

import com.databricks.spark.avro._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import scala.util.Properties

/**
  * Spark Stock Exchange Write Demo
  * @author lawrence.daniels@gmail.com
  */
object SparkStockExchangeWriteDemo extends App {

  val separator = s"[${Properties.lineSeparator}]"
  val spark = SparkSession.builder().master("local").getOrCreate()
  val rdd = spark.sparkContext.wholeTextFiles("./data/*/*.txt") flatMap { case (file, content) =>
    content.split(separator).map(_.trim) flatMap {
      case line if line.trim.isEmpty | line.contains("<") => None
      case line =>
        val exchange = new File(file).getName.take('_')
        line.split("[,]").toList match {
          case ticker :: date :: open :: high :: low :: close :: vol :: Nil =>
            val year = date.take(4).toInt
            val month = date.slice(4, 6).toInt
            Some((year, month, exchange, ticker, date.toLong, open.toDouble, high.toDouble, low.toDouble, close.toDouble, vol.toLong))
          case _ => None
        }
    } 
  }

  // create the data frame
  val df = spark.createDataFrame(rdd).toDF("year", "month", "exchange", "ticker", "date", "open", "high", "low", "close", "vol")

  val parameters = Map("recordName" -> "StockData", "recordNamespace" -> "com.databricks.spark.avro")
  val now = System.currentTimeMillis()

  writeAvro(df)
  writeCSV(df)
  writeJSON(df)
  writePaquet(df)

  def writeAvro(df: sql.DataFrame): Unit = {
    df.write
      .options(parameters)
      .partitionBy("year", "month", "exchange")
      .avro(s"./tmp/A$now")
  }

  def writeCSV(df: sql.DataFrame): Unit = {
    df.write
      .partitionBy("year", "month", "exchange")
      .csv(s"./tmp/C$now")
  }

  def writeJSON(df: sql.DataFrame): Unit = {
    df.write
      .partitionBy("year", "month", "exchange")
      .json(s"./tmp/J$now")
  }

  def writePaquet(df: sql.DataFrame): Unit = {
    df.write
      .partitionBy("year", "month", "exchange")
      .parquet(s"./tmp/P$now")
  }

  case class StockData(year: Int, month: Int, exchange: String, ticker: String, date: Long, open: Double, high: Double, low: Double, close: Double, vol: Long)

}
