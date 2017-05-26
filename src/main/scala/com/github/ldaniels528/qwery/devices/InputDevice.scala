package com.github.ldaniels528.qwery.devices

import scala.io.Source

/**
  * Input Device
  * @author lawrence.daniels@gmail.com
  */
trait InputDevice extends Device {

  def getSize: Option[Long]

  def read(): Option[Record]

  def toIterator: Iterator[Record] = new Iterator[Record] {
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

/**
  * Input Device Companion
  * @author lawrence.daniels@gmail.com
  */
object InputDevice {

  /**
    * Source Enrichment
    * @param source the given [[Source]]
    */
  implicit class SourceEnrichment(val source: Source) extends AnyVal {

    @inline
    def getNonEmptyLines: Iterator[String] = source.getLines().filter(_.trim.nonEmpty)

  }

}