package com.qwery.platform.spark

import org.apache.spark.sql.DataFrame

/**
  * Union Operation for Spark
  * @author lawrence.daniels@gmail.com
  */
case class SparkUnion(query0: SparkInvokable, query1: SparkInvokable, alias: Option[String]) extends SparkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
    for {
      df0 <- query0.execute(input)
      df1 <- query1.execute(input)
    } yield df0 union df1
  }
}
