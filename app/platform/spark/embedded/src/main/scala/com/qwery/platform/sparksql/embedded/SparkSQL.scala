package com.qwery.platform.sparksql.embedded

import org.apache.spark.sql.DataFrame

/**
  * Represents a code block (e.g. BEGIN ... END)
  * @param ops one or more [[SparkInvokable operations]] to execute
  */
case class SparkSQL(ops: List[SparkInvokable]) extends SparkInvokable {
  override def execute(input: Option[DataFrame] = None)(implicit rc: EmbeddedSparkContext): Option[DataFrame] = {
    ops.foldLeft(input) { (df, op) =>
      op.execute(df)
    }
  }
}

/**
  * Spark SQL Companion
  * @author lawrence.daniels@gmail.com
  */
object SparkSQL {

  /**
    * Creates a new SQL code block to sequentially execute the given operations
    * @param ops the given collection of [[SparkInvokable operations]]
    * @return a new [[SparkSQL SQL code block]]
    */
  def apply(ops: SparkInvokable*): SparkSQL = new SparkSQL(ops.toList)

}