package com.qwery.database
package server

import com.qwery.database.JSONSupport.JSONProductConversion
import com.qwery.database.device.BlockDevice
import com.qwery.database.models.TableColumn
import com.qwery.database.models.TableColumn._
import org.slf4j.Logger

package object models {

  case class ColumnSearchResult(databaseName: String, tableName: String, column: TableColumn) {
    override def toString: String = this.toJSON
  }

  case class DatabaseSearchResult(databaseName: String) {
    override def toString: String = this.toJSON
  }

  case class DatabaseSummary(databaseName: String, tables: Seq[TableSummary]) {
    override def toString: String = this.toJSON
  }

  case class LoadMetrics(records: Long, ingestTime: Double, recordsPerSec: Double) {
    override def toString: String = this.toJSON
  }

  case class QueryResult(databaseName: String,
                         tableName: String,
                         columns: Seq[TableColumn] = Nil,
                         rows: Seq[Seq[Option[Any]]] = Nil,
                         count: Int = 0,
                         __ids: Seq[Int] = Nil) {

    def foreachKVP(f: KeyValues => Unit): Unit = {
      val columnNames = columns.map(_.name)
      rows foreach { values =>
        val kvp = KeyValues((columnNames zip values).flatMap { case (key, value_?) => value_?.map(value => key -> value) }: _*)
        f(kvp)
      }
    }

    def show(implicit logger: Logger): Unit = {
      logger.info(columns.map(c => f"${c.name}%-12s").mkString(" | "))
      logger.info(columns.map(_ => "-" * 12).mkString("-+-"))
      for (row <- rows) logger.info(row.map(v => f"${v.orNull}%-12s").mkString(" | "))
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

  case class TableCreation(tableName: String, description: Option[String], columns: Seq[TableColumn], isColumnar: Boolean) {
    override def toString: String = this.toJSON
  }

  object TableCreation {
    def create(tableName: String, description: Option[String], columns: Seq[Column], isColumnar: Boolean): TableCreation = {
      new TableCreation(tableName, description, columns.map(_.toTableColumn), isColumnar)
    }
  }

  case class TableMetrics(databaseName: String,
                          tableName: String,
                          columns: Seq[TableColumn],
                          physicalSize: Option[Long],
                          recordSize: Int,
                          rows: ROWID) {
    override def toString: String = this.toJSON
  }

  case class TableSearchResult(databaseName: String, tableName: String) {
    override def toString: String = this.toJSON
  }

  case class TableSummary(tableName: String, tableType: String, description: Option[String]) {
    override def toString: String = this.toJSON
  }

  case class UpdateCount(count: Int, __id: Option[Int] = None) {
    override def toString: String = this.toJSON
  }

}
