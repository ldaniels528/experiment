package com.qwery.platform.flink

/**
  * Flink SHOW implementation
  * @param rows the given [[FlinkInvokable]]
  * @param limit the optional limit
  */
case class FlinkShow(rows: FlinkInvokable, limit: Option[Int]) extends FlinkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
    rows.execute(input) map { df =>
      limit match {
        case Some(n) => df.print(n)
        case None => df.print()
      }
      df
    }
  }
}