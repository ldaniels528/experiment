package com.qwery.platform.flink

/**
  * Represents a code block (e.g. BEGIN ... END)
  * @param ops one or more [[FlinkInvokable operations]] to execute
  */
case class FlinkSQL(ops: List[FlinkInvokable]) extends FlinkInvokable {

  override def execute(input: Option[DataFrame] = None)(implicit rc: FlinkQweryContext): Option[DataFrame] =
    ops.foldLeft(input) { (df, op) => op.execute(df) }

}

/**
  * Flink SQL Companion
  * @author lawrence.daniels@gmail.com
  */
object FlinkSQL {

  /**
    * Creates a new SQL code block to sequentially execute the given operations
    * @param ops the given collection of [[FlinkInvokable operations]]
    * @return a new [[FlinkSQL SQL code block]]
    */
  def apply(ops: FlinkInvokable*): FlinkSQL = new FlinkSQL(ops.toList)

}