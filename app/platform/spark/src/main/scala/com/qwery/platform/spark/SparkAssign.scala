package com.qwery.platform.spark

import com.qwery.models.expressions.VariableRef
import org.apache.spark.sql.DataFrame

/**
  * Spark Variable Assignment
  * @param variable the given [[VariableRef]]
  * @param value the given [[SparkInvokable]]
  */
case class SparkAssign(variable: VariableRef, value: SparkInvokable) extends SparkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
    val result = value.execute(input)
    rc.updateDataSet(variable.name, result)
    result
  }
}