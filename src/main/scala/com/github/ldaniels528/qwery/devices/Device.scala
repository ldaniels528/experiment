package com.github.ldaniels528.qwery.devices

import com.github.ldaniels528.qwery.ops.Scope

/**
  * Represents an I/O device
  * @author lawrence.daniels@gmail.com
  */
trait Device {

  def open(scope: Scope): Unit

  def close(): Unit

}
