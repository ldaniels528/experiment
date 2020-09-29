package com.qwery.database.server

import com.qwery.database.ROWID
import com.qwery.database.server.TableService.UpdateResult

/**
 * Table Service
 */
trait TableService[R] {

  def appendRow(tableName: String, row: R): UpdateResult

  def deleteRow(tableName: String, rowID: ROWID): UpdateResult

  def dropTable(tableName: String): UpdateResult

  def executeQuery(sql: String): Seq[R]

  def findRows(tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[R]

  def getRow(tableName: String, rowID: ROWID): Option[R]

  def getRows(tableName: String, start: ROWID, length: ROWID): Seq[R]

  def getStatistics(tableName: String): TableFile.TableStatistics

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