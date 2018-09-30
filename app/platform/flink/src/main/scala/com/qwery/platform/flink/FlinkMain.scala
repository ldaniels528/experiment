package com.qwery.platform.flink

case class FlinkMain(name: String, code: FlinkInvokable, streaming: Boolean) extends FlinkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
    rc.mainProgram = Option(this)
    val result = code.execute(input)
    rc.start()
    result
  }
}