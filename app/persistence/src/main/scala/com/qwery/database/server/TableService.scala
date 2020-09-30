package com.qwery.database.server

import com.qwery.database.ROWID
import com.qwery.database.server.TableService.UpdateResult

/**
 * Table Service
 */
trait TableService[R] {

  def appendRow(databaseName: String, tableName: String, row: R): UpdateResult

  def deleteRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): UpdateResult

  def deleteRow(databaseName: String, tableName: String, rowID: ROWID): UpdateResult

  def dropTable(databaseName: String, tableName: String): UpdateResult

  def executeQuery(databaseName: String, sql: String): Seq[R]

  def findRows(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[R]

  def getDatabaseMetrics(databaseName: String): TableFile.DatabaseMetrics

  def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[R]

  def getRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): Seq[R]

  def getTableMetrics(databaseName: String, tableName: String): TableFile.TableMetrics

}

/**
 * Table Service Companion
 */
object TableService {

  case class UpdateResult(count: Int,
                          responseTime: Double,
                          rowID: Option[Int] = None) {
    override def toString: String = {
      f"{ count: $count, responseTime: $responseTime%.1f ${rowID.map(id => s", rowID: $id ").getOrElse("")}}"
    }
  }

}