package com.qwery.platform.spark

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * Spark Variable Assignment
  * @param name  the given variable name
  * @param value the given [[SparkInvokable]]
  */
case class SparkSetLocalVariable(name: String, value: SparkQweryContext => Any) extends SparkInvokable {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
    logger.info(s"Setting local variable '$name' to '$value'...")
    rc.setVariable(name, value(rc))
    None
  }
}