package com.qwery.platform.flink

import com.qwery.models.Column
import com.qwery.models.expressions.Expression
import org.slf4j.LoggerFactory

/**
  * Flink Procedure
  * @param name   the name of the procedure
  * @param params the procedure's parameters
  * @param code   the procedure's code
  */
case class FlinkProcedure(name: String, params: Seq[Column], code: FlinkInvokable) {
  private val logger = LoggerFactory.getLogger(getClass)

  def invoke(args: List[Expression])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
    import com.qwery.util.OptionHelper.Implicits.Risky._

    val inputArgs = params zip args map { case (param, arg) => param.name -> arg.asString } // TODO fix type issue
    logger.info(s"inputArgs => ${inputArgs map { case (k, v) => s"('$k', '$v')" } mkString ", "}")
    code.execute(input = None /*inputArgs.toDF()*/) // TODO fix me!
  }

}