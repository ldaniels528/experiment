package com.qwery.database.models

import com.qwery.database.JSONSupport._
import com.qwery.database.KeyValues
import com.qwery.database.device.BlockDevice
import com.qwery.database.files.TableColumn
import com.qwery.database.files.TableColumn._
import org.slf4j.Logger

case class QueryResult(databaseName: String,
                       tableName: String,
                       columns: Seq[TableColumn] = Nil,
                       rows: Seq[Seq[Option[Any]]] = Nil,
                       count: Long = 0,
                       __ids: Seq[Long] = Nil) {

  def foreachKVP(f: KeyValues => Unit): Unit = {
    val columnNames = columns.map(_.name)
    rows foreach { values =>
      val kvp = KeyValues((columnNames zip values).flatMap { case (key, value_?) => value_?.map(value => key -> value) }: _*)
      f(kvp)
    }
  }

  def show(limit: Int = 20)(implicit logger: Logger): Unit = {
    logger.info(columns.map(c => f"${c.name}%-12s").mkString(" | "))
    logger.info(columns.map(_ => "-" * 12).mkString("-+-"))
    for (row <- rows.take(limit)) logger.info(row.map(v => f"${v.orNull}%-12s").mkString(" | "))
  }

  override def toString: String = this.toJSON
}

object QueryResult {
  def toQueryResult(databaseName: String, tableName: String, device: BlockDevice): QueryResult = {
    val rows = device.toList
    QueryResult(databaseName, tableName, columns = device.columns.map(_.toTableColumn), __ids = rows.map(_.id), rows = rows map { row =>
      val mapping = row.toMap
      device.columns.map(_.name).map(mapping.get)
    })
  }
}