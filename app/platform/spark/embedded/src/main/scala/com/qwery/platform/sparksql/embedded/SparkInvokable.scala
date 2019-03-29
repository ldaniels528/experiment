package com.qwery.platform.sparksql.embedded

import org.apache.spark.sql.DataFrame

/**
  * Represents an executable Spark Operation
  * @author lawrence.daniels@gmail.com
  */
trait SparkInvokable {

  def execute(input: Option[DataFrame])(implicit rc: EmbeddedSparkContext): Option[DataFrame]

}
