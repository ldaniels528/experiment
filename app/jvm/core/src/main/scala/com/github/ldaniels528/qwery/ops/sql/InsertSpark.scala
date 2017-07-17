package com.github.ldaniels528.qwery.ops.sql

import com.github.ldaniels528.qwery.util.TupleHelper._
import com.databricks.spark.avro._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.sources.DataResource
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Represents a SPARK INSERT statement
  * @author lawrence.daniels@gmail.com
  */
case class InsertSpark(target: DataResource, fields: Seq[Field], source: Executable) extends Executable {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute(scope: Scope): ResultSet = {
    Try {
      val master = target.hints.flatMap(_.sparkMaster).getOrElse("local[*]")
      val spark = SparkSession.builder().master(master).getOrCreate()
      logger.info("Retrieving data stream...")
      val resultSet = source.execute(scope).toStream
      val columns = fields.map(_.name)
      logger.info(s"Tuplizing data stream (${columns.size} columns)...")
      import spark.implicits._

      val dataFrame = columns.length match {
        case 2 => spark.sparkContext.parallelize(resultSet map (flatten(_).toTuple2), numSlices = 1).toDF(columns: _*)
        case 3 => spark.sparkContext.parallelize(resultSet map (flatten(_).toTuple3), numSlices = 1).toDF(columns: _*)
        case 4 => spark.sparkContext.parallelize(resultSet map (flatten(_).toTuple4), numSlices = 1).toDF(columns: _*)
        case 5 => spark.sparkContext.parallelize(resultSet map (flatten(_).toTuple5), numSlices = 1).toDF(columns: _*)
        case 6 => spark.sparkContext.parallelize(resultSet map (flatten(_).toTuple6), numSlices = 1).toDF(columns: _*)
        case 7 => spark.sparkContext.parallelize(resultSet map (flatten(_).toTuple7), numSlices = 1).toDF(columns: _*)
        case 8 => spark.sparkContext.parallelize(resultSet map (flatten(_).toTuple8), numSlices = 1).toDF(columns: _*)
        case 9 => spark.sparkContext.parallelize(resultSet map (flatten(_).toTuple9), numSlices = 1).toDF(columns: _*)
        case n =>
          throw new IllegalStateException(s"Data with $n columns could not be tupled")
      }

      logger.info("Creating to data frame...")
      val outputPath = target.getRealPath(scope)
      logger.info(s"Writing to $outputPath")
      target.hints.getOrElse(Hints()) match {
        case hints if hints.avro.isDefined => writeAvro(outputPath, dataFrame)
        case hints if hints.delimiter.isDefined => writeCSV(outputPath, dataFrame)
        case hints if hints.isJson.contains(true) => writeJSON(outputPath, dataFrame)
        case _ => writeCSV(outputPath, dataFrame)
      }
      ResultSet.inserted(count = 0, statistics = None)
    } match {
      case Success(results) => results
      case Failure(e) =>
        e.printStackTrace()
        ResultSet()
    }
  }

  private def flatten(row: Row): Seq[String] = row.columns.map(_._2).map {
    case null => null
    case x => x.toString
  }

  private def writeAvro(outputPath: String, df: sql.DataFrame): Unit = {
    df.write
      //.partitionBy("year", "month")
      .avro(outputPath)
  }

  private def writeCSV(outputPath: String, df: sql.DataFrame): Unit = {
    df.write
      //.partitionBy("year", "month")
      .csv(outputPath)
  }

  private def writeJSON(outputPath: String, df: sql.DataFrame): Unit = {
    df.write
      //.partitionBy("year", "month")
      .json(outputPath)
  }

  private def writeParquet(outputPath: String, df: sql.DataFrame): Unit = {
    df.write
      //.partitionBy("year", "month")
      .parquet(outputPath)
  }

}
