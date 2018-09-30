package com.qwery.platform
package flink

/**
  * Represents an executable Flink Operation
  * @author lawrence.daniels@gmail.com
  */
trait FlinkInvokable extends PlatformInvokable[DataFrame] {

  def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame]

}
