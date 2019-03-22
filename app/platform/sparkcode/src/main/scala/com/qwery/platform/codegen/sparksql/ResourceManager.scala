package com.qwery
package platform.codegen.sparksql

import java.util.concurrent.atomic.AtomicBoolean

import com.databricks.spark.avro._
import com.qwery.models.StorageFormats._
import com.qwery.models._
import com.qwery.platform.spark.{SparkQweryCompiler, die}
import com.qwery.util.ConversionHelper._
import com.qwery.util.OptionHelper._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
  * Resource Manager
  * @author lawrence.daniels@gmail.com
  */
object ResourceManager {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private val tables = TrieMap[String, TableLike]()
  private val dataFrames = TrieMap[String, DataFrame]()

  /**
    * Adds a table or view to resource management
    * @param tableOrView the given [[TableLike table or view]]
    */
  def add(tableOrView: TableLike)(implicit spark: SparkSession): Unit = {
    logger.info(s"Registering ${tableOrView.getClass.getSimpleName} '${tableOrView.name}'...")
    tables.put(tableOrView.name, tableOrView)
    dataFrames.put(tableOrView.name, {
      val df = read(tableOrView)
      df.createOrReplaceGlobalTempView(tableOrView.name)
      df
    })
  }

  def apply(name: String): TableLike = tables.getOrElse(name, die(s"Table or view '$name' was not found"))

  /**
    * Creates a data set from the given source
    * @param columns the column definitions
    * @param data    the given collection of rows
    * @return the option of a [[DataFrame]]
    */
  def createDataSet(columns: Seq[Column], data: Seq[Seq[Any]])(implicit spark: SparkSession): DataFrame = {
    val rows = data.map(values => Row(values: _*))
    val rdd = spark.sparkContext.makeRDD(rows)
    spark.sqlContext.createDataFrame(rdd, createSchema(columns))
  }

  def createSchema(columns: Seq[Column]): StructType = {
    import SparkQweryCompiler.Implicits._
    StructType(fields = columns.map(_.compile))
  }

  /**
    * Returns the equivalent query operation to represent the given table or view
    * @param tableName the given table name
    * @param spark     the implicit [[SparkSession]]
    * @return the [[DataFrame]]
    */
  def read(tableName: String)(implicit spark: SparkSession): DataFrame = read(apply(tableName))

  /**
    * Returns the equivalent query operation to represent the given table or view
    * @param tableOrView the given [[TableLike table or view]]
    * @param spark       the implicit [[SparkSession]]
    * @return the [[DataFrame]]
    */
  def read(tableOrView: TableLike)(implicit spark: SparkSession): DataFrame = {
    import SparkQweryCompiler.Implicits._
    dataFrames.getOrElseUpdate(tableOrView.name, {
      logger.info(s"Loading data frame for '${tableOrView.name}'...")
      tableOrView match {
        /*
        case ref@InlineTable(name, columns, source) =>
          createDataSet(columns, source.compile match {
            case spout: SparkInsert.Spout => spout.copy(resolver = Option(SparkTableColumnResolver(ref)))
            case other => other
          }).map { df => df.createOrReplaceTempView(name); df }*/
        case table: Table =>
          val reader = spark.read.tableOptions(table)
          table.inputFormat.orFail("Table input format was not specified") match {
            case AVRO => reader.avro(table.location)
            case CSV => reader.schema(createSchema(table.columns)).csv(table.location)
            case JDBC => reader.jdbc(table.location, table.name, table.properties.toProperties)
            case JSON => reader.json(table.location)
            case PARQUET => reader.parquet(table.location)
            case ORC => reader.orc(table.location)
            case format => die(s"Storage format $format is not supported for reading")
          }
        //case view: View => view.query.compile.execute(input = None)
        case unknown => die(s"Unrecognized table type '$unknown' (${unknown.getClass.getName})")
      }
    })
  }

  /**
    * Writes the source data frame to the given target
    * @param source      the source [[DataFrame]]
    * @param destination the [[TableLike destination table or view]]
    * @param append      indicates whether the destination should be appended (or conversely overwritten)
    * @param spark       the implicit [[SparkSession]]
    */
  def write(source: DataFrame, destination: TableLike, append: Boolean)(implicit spark: SparkSession): Unit = {
    import SparkQweryCompiler.Implicits._
    destination match {
      case table: InlineTable => die(s"Inline table '${table.name}' is read-only")
      case table: Table =>
        val writer = source.write.tableOptions(table).mode(if (append) SaveMode.Append else SaveMode.Overwrite)
        table.outputFormat.orFail("Table output format was not specified") match {
          case AVRO => writer.avro(table.location)
          case CSV => writer.csv(table.location)
          case JDBC => writer.jdbc(table.location, table.name, table.properties.toProperties)
          case JSON => writer.json(table.location)
          case PARQUET => writer.parquet(table.location)
          case ORC => writer.orc(table.location)
          case format => die(s"Storage format '$format' is not supported for writing")
        }
      case view: View => die(s"View '${view.name}' is read-only")
    }
  }

}
