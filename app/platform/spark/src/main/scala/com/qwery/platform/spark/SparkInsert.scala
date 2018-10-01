package com.qwery.platform.spark

import com.qwery.models.Location
import com.qwery.models.expressions.Expression
import org.apache.spark.sql.DataFrame

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
    * @param rows     the given rows of data
    * @param resolver the optional [[SparkColumnResolver]]
    */
  case class Spout(rows: List[List[Any]], resolver: Option[SparkColumnResolver]) extends SparkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
      resolver map { aResolver =>
        rc.createDataSet(columns = aResolver.resolve, data = rows)
      }
    }
  }

}