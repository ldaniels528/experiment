package com.qwery.platform.sparksql.embedded

import com.qwery.models.expressions.VariableRef
import com.qwery.platform.sparksql.embedded.SparkMainProgram.StreamingOptions
import org.apache.spark.sql.DataFrame

/**
  * Spark Application Entry Point
  * @author lawrence.daniels@gmail.com
  */
case class SparkMainProgram(name: String,
                            code: SparkInvokable,
                            arguments: Option[VariableRef],
                            environment: Option[VariableRef],
                            hiveSupport: Boolean,
                            streaming: Boolean,
                            streamingOptions: Option[StreamingOptions] = None
                           ) extends SparkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: EmbeddedSparkContext): Option[DataFrame] = {
    rc.init(Option(this))
    code.execute(input)
  }
}

/**
  * Spark Main Program
  * @author lawrence.daniels@gmail.com
  */
object SparkMainProgram {

  /**
    * Spark Streaming Options
    * @param batchDuration the optional batch duration
    */
  case class StreamingOptions(batchDuration: Option[Long])

}