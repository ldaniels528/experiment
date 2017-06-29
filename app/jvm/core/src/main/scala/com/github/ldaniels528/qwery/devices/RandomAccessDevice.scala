package com.github.ldaniels528.qwery.devices

/**
  * Random Access Device
  * @author lawrence.daniels@gmail.com
  */
trait RandomAccessDevice extends InputDevice {

  def fastForward(partitions: Seq[Int]): Unit

  def rewind(partitions: Seq[Int]): Unit

  def seek(offset: Long, partition: Int): Unit

}
