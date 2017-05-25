package com.github.ldaniels528.qwery.devices

/**
  * Represents an I/O record
  * @author lawrence.daniels@gmail.com
  */
trait Record {

  /**
    * Returns the offset of the record
    * @return the offset
    */
  def offset: Long

  /**
    * Returns the record's data
    * @return the record's data
    */
  def data: Array[Byte]

}

/**
  * Record Companion
  * @author lawrence.daniels@gmail.com
  */
object Record {

  def apply(offset: Long, data: Array[Byte]) = RecordImpl(offset, data)

  def unapply(record: Record): Option[(Long, Array[Byte])] = Some((record.offset, record.data))

  case class RecordImpl(offset: Long, data: Array[Byte]) extends Record

}
