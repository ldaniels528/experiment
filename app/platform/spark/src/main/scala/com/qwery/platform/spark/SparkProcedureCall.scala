package com.qwery.platform.spark

import com.qwery.models.Aliasable
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * Invokes a procedure by name
  * @param name the name of the procedure
  * @param args the given collection of arguments to be passed to the procedure upon invocation
  */
case class SparkProcedureCall(name: String, args: List[Any]) extends SparkInvokable with Aliasable {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
    logger.info(s"Invoking procedure $name....")
    rc.getProcedure(name).invoke(args)
  }
}