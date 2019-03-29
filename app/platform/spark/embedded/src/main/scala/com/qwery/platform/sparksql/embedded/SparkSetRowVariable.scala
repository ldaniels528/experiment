package com.qwery.platform.sparksql.embedded

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * Spark Row Variable Assignment
  * @param name  the given variable name
  * @param value the given [[SparkInvokable]]
  */
case class SparkSetRowVariable(name: String, value: SparkInvokable) extends SparkInvokable {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute(input: Option[DataFrame])(implicit rc: EmbeddedSparkContext): Option[DataFrame] = {
    logger.info(s"Setting row variable '$name'")
    rc.updateDataSet(name, value.execute(input))
  }
}
