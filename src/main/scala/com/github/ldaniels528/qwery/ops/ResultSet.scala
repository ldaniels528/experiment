package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.sources.Statistics

/**
  * Represents an SQL Result Set
  * @author lawrence.daniels@gmail.com
  */
case class ResultSet(rows: Iterator[Seq[Column]] = Iterator.empty, statistics: Option[Statistics] = None)
  extends Iterator[Row] {

  override def hasNext: Boolean = rows.hasNext

  override def next(): Row = rows.next()

}

/**
  * Result Set Companion
  * @author lawrence.daniels@gmail.com
  */
object ResultSet {

  def affected(count: Long = 1) = ResultSet(rows = Iterator(Seq("ROWS_AFFECTED" -> count)))

  def inserted(count: Long, statistics: Option[Statistics]): ResultSet = {
    ResultSet(rows = Iterator(Seq("ROWS_INSERTED" -> count)), statistics)
  }

  def updated(count: Long, statistics: Option[Statistics]): ResultSet = {
    ResultSet(rows = Iterator(Seq("ROWS_UPDATED" -> count)), statistics)
  }

}