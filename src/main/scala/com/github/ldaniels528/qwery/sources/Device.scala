package com.github.ldaniels528.qwery.sources

/**
  * Represents an I/O device
  * @author lawrence.daniels@gmail.com
  */
trait Device {

  def open(): Unit

  def close(): Unit

}
