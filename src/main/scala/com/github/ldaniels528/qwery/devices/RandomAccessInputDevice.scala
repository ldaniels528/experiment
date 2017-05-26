package com.github.ldaniels528.qwery.devices

/**
  * Random Access Input Device
  * @author lawrence.daniels@gmail.com
  */
trait RandomAccessInputDevice extends InputDevice {

  def fastForward(partitions: Seq[Int]): Unit

  def rewind(partitions: Seq[Int]): Unit

  def seek(offset: Long, partition: Int): Unit

}
