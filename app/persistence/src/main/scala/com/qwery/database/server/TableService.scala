package com.qwery.database.server

import com.qwery.database.ROWID
import com.qwery.database.server.JSONSupport.JSONProductConversion
import com.qwery.database.server.TableService.UpdateResult

/**
 * Table Service
 */
trait TableService[R] {

  def appendRow(databaseName: String, tableName: String, values: TupleSet): UpdateResult

  def deleteRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): UpdateResult

  def deleteRow(databaseName: String, tableName: String, rowID: ROWID): UpdateResult

  def dropTable(databaseName: String, tableName: String): UpdateResult

  def executeQuery(databaseName: String, sql: String): Seq[R]

  def findRows(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[R]

  def getDatabaseMetrics(databaseName: String): TableFile.DatabaseMetrics

  def getLength(databaseName: String, tableName: String): UpdateResult

  def getRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): Seq[R]

  def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[R]

  def getTableMetrics(databaseName: String, tableName: String): TableFile.TableMetrics

  def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): UpdateResult

}

/**
 * Table Service Companion
 */
object TableService {

  case class UpdateResult(count: Int,
                          responseTime: Double,
                          __id: Option[Int] = None) {
    override def toString: String = this.toJSON
  }

}