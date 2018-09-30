package com.qwery.platform.spark

import com.qwery.models.Insert.DataRow
import com.qwery.models.Location
import com.qwery.models.expressions.Expression
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory

/**
  * Represents a SQL-like Insert operation
  * @param destination the given [[SparkInvokable destination]]
  * @param source      the given [[SparkInvokable source]]
  * @param fields      the fields to insert into the table
  */
case class SparkInsert(destination: SparkInvokable, source: SparkInvokable, fields: Seq[Expression]) extends SparkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] =
    destination.execute(source.execute(input))
}

/**
  * Insert Operation Companion
  * @author lawrence.daniels@gmail.com
  */
object SparkInsert {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /**
    * Represents a writable sink
    * @param target the given [[Location target]]
    * @param append indicates whether the data should be appended
    */
  case class Sink(target: Location, append: Boolean) extends SparkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = input map { df =>
      SparkQweryCompiler.write(df, target, append)
      df
    }
  }

  /**
    * Represents a readable spout
    * @param values the given readable values
    * @param resolver the optional [[SparkColumnResolver]]
    */
  case class Spout(values: List[DataRow], resolver: Option[SparkColumnResolver] = None) extends SparkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
      import SparkQweryCompiler.Implicits._
      resolver map { theResolver =>
        val columns = theResolver.resolve
        val rows = values.map(row => Row(row.map(_.asAny): _*))
        val rdd = rc.spark.sparkContext.makeRDD(rows)
        val schema = StructType(fields = columns.map(_.compile))
        logger.info(s"schema: $schema")
        rc.spark.sqlContext.createDataFrame(rdd, schema)
      }
    }
  }

}