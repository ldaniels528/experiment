package com.qwery.database.server

import com.qwery.database.JSONSupport.JSONProductConversion
import com.qwery.database.device.BlockDevice
import com.qwery.database.models.TableColumn
import com.qwery.database.models.TableColumn._
import com.qwery.database.{Column, KeyValues, ROWID}

package object models {

  case class ColumnSearchResult(databaseName: String, tableName: String, column: TableColumn)

  case class DatabaseSearchResult(databaseName: String) {
    override def toString: String = this.toJSON
  }

  case class DatabaseMetrics(databaseName: String, tables: Seq[String]) {
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

    override def toString: String = this.toJSON
  }

  object QueryResult {
    def toQueryResult(databaseName: String, tableName: String, out: BlockDevice): QueryResult = {
      val rows = out.toList
      val dstFieldNames: Set[String] = out.columns.map(_.name).toSet
      QueryResult(databaseName, tableName, columns = out.columns.map(_.toTableColumn), __ids = rows.map(_.id), rows = rows map { row =>
        val mapping = row.toMap.filter { case (name, _) => dstFieldNames.contains(name) } // TODO properly handle field projection
        out.columns map { column => mapping.get(column.name) }
      })
    }
  }

  case class TableCreation(tableName: String, columns: Seq[TableColumn])

  object TableCreation {
    def create(tableName: String, columns: Seq[Column]) = new TableCreation(tableName, columns.map(_.toTableColumn))
  }

  case class TableMetrics(databaseName: String,
                          tableName: String,
                          columns: Seq[TableColumn],
                          physicalSize: Option[Long],
                          recordSize: Int,
                          rows: ROWID) {
    override def toString: String = this.toJSON
  }

  case class TableSearchResult(databaseName: String, tableName: String)

  case class UpdateCount(count: Int, __id: Option[Int] = None) {
    override def toString: String = this.toJSON
  }

}
