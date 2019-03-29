package com.qwery.platform.sparksql.embedded

import com.qwery.platform.PlatformInvokable
import org.apache.spark.sql.DataFrame

/**
  * Represents an executable Spark Operation
  * @author lawrence.daniels@gmail.com
  */
trait SparkInvokable extends PlatformInvokable[DataFrame] {

  def execute(input: Option[DataFrame])(implicit rc: EmbeddedSparkContext): Option[DataFrame]

}
