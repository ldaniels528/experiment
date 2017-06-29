package com.github.ldaniels528.qwery.devices

/**
  * Represents an I/O record
  * @param data the record's data (in bytes)
  * @param offset the offset of the record
  * @param partition the partition of the record
  */
case class Record(data: Array[Byte], offset: Long, partition: Int = 0)
