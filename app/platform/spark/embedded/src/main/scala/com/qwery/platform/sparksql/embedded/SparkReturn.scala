package com.qwery.platform.sparksql.embedded

import org.apache.spark.sql.DataFrame

/**
  * RETURN statement for Spark
  * @param value the given return value
  */
case class SparkReturn(value: Option[SparkInvokable]) extends SparkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: EmbeddedSparkContext): Option[DataFrame] =
    value.flatMap(_.execute(input))
}
