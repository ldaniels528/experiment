package com.github.ldaniels528.qwery.devices

/**
  * Represents an I/O record
  * @author lawrence.daniels@gmail.com
  */
trait Record {

  /**
    * Returns the record's data
    * @return the record's data
    */
  def data: Array[Byte]

  /**
    * Returns the offset of the record
    * @return the offset
    */
  def offset: Long

  /**
    * Returns the partition of the record
    * @return the partition
    */
  def partition: Int

}

/**
  * Record Companion
  * @author lawrence.daniels@gmail.com
  */
object Record {

  def apply(data: Array[Byte], offset: Long, partition: Int = 0) = RecordImpl(data, offset, partition)

  def unapply(record: Record): Option[(Array[Byte], Long, Int)] = Some((record.data, record.offset, record.partition))

  case class RecordImpl(data: Array[Byte], offset: Long, partition: Int) extends Record

}
