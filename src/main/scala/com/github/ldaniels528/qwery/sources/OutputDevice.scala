package com.github.ldaniels528.qwery.sources

/**
  * Output Device
  * @author lawrence.daniels@gmail.com
  */
trait OutputDevice extends Device {

  def write(record: Record): Unit

}
