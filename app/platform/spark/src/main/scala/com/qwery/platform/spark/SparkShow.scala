package com.qwery.platform.spark

import org.apache.spark.sql.DataFrame

/**
  * Spark SHOW implementation
  * @param rows the given [[SparkInvokable]]
  * @param limit the optional limit
  */
case class SparkShow(rows: SparkInvokable, limit: Option[Int]) extends SparkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
    rows.execute(input) map { df =>
      limit match {
        case Some(n) => df.show(n)
        case None => df.show()
      }
      df
    }
  }
}