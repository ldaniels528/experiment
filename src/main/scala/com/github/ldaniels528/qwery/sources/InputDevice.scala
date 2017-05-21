package com.github.ldaniels528.qwery.sources

/**
  * Input Device
  * @author lawrence.daniels@gmail.com
  */
trait InputDevice extends Device {

  def read(): Option[Record]

  def toIterator: Iterator[Record] = new Iterator[Record] {
    open()

    private var nextRecord_? : Option[Record] = read()

    override def hasNext: Boolean = nextRecord_?.nonEmpty

    override def next(): Record = nextRecord_? match {
      case Some(record) =>
        nextRecord_? = read()
        if (nextRecord_?.isEmpty) close()
        record
      case None =>
        throw new IllegalStateException("Empty iterator")
    }
  }

}
