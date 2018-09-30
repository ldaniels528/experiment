package com.qwery.platform.spark

import com.qwery.models.Column
import com.qwery.models.expressions.Expression
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * Spark Procedure
  * @param name   the name of the procedure
  * @param params the procedure's parameters
  * @param code   the procedure's code
  */
case class SparkProcedure(name: String, params: Seq[Column], code: SparkInvokable) {
  private val logger = LoggerFactory.getLogger(getClass)

  def invoke(args: List[Expression])(implicit rc: SparkQweryContext): Option[DataFrame] = {
    import com.qwery.util.OptionHelper.Implicits.Risky._
    import rc.spark.implicits._

    val inputArgs = params zip args map { case (param, arg) => param.name -> arg.asString } // TODO fix type issue
    logger.info(s"$name <= ${inputArgs map { case (k, v) => s"('$k', '$v')" } mkString ", "}")
    code.execute(input = inputArgs.toDF())
  }

}