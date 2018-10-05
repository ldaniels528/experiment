package com.qwery.platform.flink

import com.qwery.models.expressions.VariableRef

/**
  * Flink Application Entry Point
  * @author lawrence.daniels@gmail.com
  */
case class FlinkMainProgram(name: String,
                            code: FlinkInvokable,
                            arguments: Option[VariableRef],
                            environment: Option[VariableRef],
                            hiveSupport: Boolean,
                            streaming: Boolean) extends FlinkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
    rc.mainProgram = Option(this)
    val result = code.execute(input)
    rc.start()
    result
  }
}