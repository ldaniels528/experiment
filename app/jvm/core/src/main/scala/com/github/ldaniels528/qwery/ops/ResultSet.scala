package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.util.OptionHelper._
import com.github.ldaniels528.qwery.sources.Statistics

/**
  * Represents an SQL Result Set
  * @author lawrence.daniels@gmail.com
  */
case class ResultSet(rows: Iterator[Row] = Iterator.empty, statistics: Option[Statistics] = None)
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
    ResultSet(rows = Iterator(Seq(
      "ROWS_INSERTED" -> statistics.map(_.totalRecords).getOrElse(count),
      "ROWS_REJECTED" -> statistics.map(_.failures).orZero
    )), statistics = statistics)
  }

  def updated(count: Long, statistics: Option[Statistics]): ResultSet = {
    ResultSet(rows = Iterator(Seq(
      "ROWS_UPDATED" -> statistics.map(_.totalRecords).getOrElse(count),
      "ROWS_REJECTED" -> statistics.map(_.failures).orZero
    )), statistics = statistics)
  }

  def upserted(inserted: Long, updated: Long, statistics: Option[Statistics]): ResultSet = {
    ResultSet(rows = Iterator(Seq(
      "ROWS_INSERTED" -> inserted,
      "ROWS_UPDATED" -> updated,
      "ROWS_REJECTED" -> statistics.map(_.failures).orZero
    )), statistics = statistics)
  }

}