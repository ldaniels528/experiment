package com.qwery.platform.sparksql.embedded

import com.qwery.models.expressions.{Condition, Expression}
import org.apache.spark.sql.{DataFrame, Column => SparkColumn}

/**
  * Represents a SQL UPDATE statement
  * @param source      the given [[SparkInvokable source]] to update
  * @param assignments the update assignments
  * @param where       the optional [[Condition where clause]]
  * @author lawrence.daniels@gmail.com
  */
case class SparkUpdate(source: SparkInvokable,
                       assignments: Seq[(String, Expression)],
                       where: Option[SparkColumn]) extends SparkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: EmbeddedSparkContext): Option[DataFrame] = ???
}
