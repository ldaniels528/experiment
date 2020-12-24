package com.qwery.database

import java.io.{File, PrintWriter}
import java.util.concurrent.atomic.AtomicInteger

import com.qwery.database.QueryProcessor.commands.{SelectRows, TableIORequest}
import com.qwery.database.device.BlockDevice
import com.qwery.models.OrderColumn
import com.qwery.models.expressions.{Expression, Field => SQLField}
import com.qwery.util.ResourceHelper._

import scala.io.Source

/**
  * Represents a virtual table file (e.g. view)
  * @param databaseName the name of the database
  * @param viewName     the name of the virtual table
  * @param device       the [[BlockDevice materialized device]]
  */
case class VirtualTableFile(databaseName: String, viewName: String, device: BlockDevice) {
  private val selector = new TableQuery(device)

  /**
    * Retrieves rows matching the given condition up to the optional limit
    * @param condition the given [[KeyValues condition]]
    * @param limit     the optional limit
    * @return the [[BlockDevice results]]
    */
  def getRows(condition: KeyValues, limit: Option[Int] = None): BlockDevice = {
    implicit val results: BlockDevice = createTempTable(device.columns)
    val count = new AtomicInteger()
    device.whileRow(condition) { row =>
      results.writeRow(row.toBinaryRow)
      limit.isEmpty || limit.exists(_ > count.addAndGet(1))
    }
    results
  }

  /**
    * Executes a query
    * @param fields  the [[Expression field projection]]
    * @param where   the condition which determines which records are included
    * @param groupBy the optional aggregation columns
    * @param orderBy the columns to order by
    * @param limit   the optional limit
    * @return a [[BlockDevice]] containing the rows
    */
  def selectRows(fields: Seq[Expression],
                 where: KeyValues,
                 groupBy: Seq[SQLField] = Nil,
                 orderBy: Seq[OrderColumn] = Nil,
                 limit: Option[Int] = None): BlockDevice = {
    selector.select(fields, where, groupBy, orderBy, limit)
  }

}

/**
  * View File Companion
  */
object VirtualTableFile {

  def apply(databaseName: String, viewName: String): VirtualTableFile = {
    new VirtualTableFile(databaseName, viewName, device = getViewDevice(databaseName, viewName))
  }

  def createView(databaseName: String, viewName: String, queryString: String): VirtualTableFile = {
    val viewFile = getViewConfigFile(databaseName, viewName)
    if (viewFile.exists()) die(s"View '$viewName' already exists")
    else writeViewConfig(databaseName, viewName, queryString)
    VirtualTableFile(databaseName, viewName)
  }

  def dropView(databaseName: String, viewName: String, ifExists: Boolean): Boolean = {
    val viewFile = getViewConfigFile(databaseName, viewName)
    viewFile.exists() && getViewConfigFile(databaseName, viewName).delete()
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  VIEW CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getViewConfigFile(databaseName: String, viewName: String): File = {
    new File(new File(getServerRootDirectory, databaseName), s"$viewName.sql")
  }

  def getViewDevice(databaseName: String, viewName: String): BlockDevice = {
    SQLCompiler.compile(databaseName, sql = readViewConfig(databaseName, viewName)) match {
      case SelectRows(_, tableName, fields, where, groupBy, orderBy, limit) =>
        TableFile(databaseName, tableName).selectRows(fields, where, groupBy, orderBy, limit)
      case other => die(s"Unhandled view query model - $other")
    }
  }

  def isVirtualTable(databaseName: String, viewName: String): Boolean = {
    getViewConfigFile(databaseName, viewName).exists()
  }

  def isVirtualTable(command: TableIORequest): Boolean = {
    getViewConfigFile(command.databaseName, command.tableName).exists()
  }

  def readViewConfig(databaseName: String, viewName: String): String = {
    val file = getViewConfigFile(databaseName, viewName)
    if (!file.exists()) die(s"Table '$viewName' does not exist") else Source.fromFile(file).use(_.mkString)
  }

  def writeViewConfig(databaseName: String, viewName: String, queryString: String): Unit = {
    new PrintWriter(getViewConfigFile(databaseName, viewName)).use(_.println(queryString))
  }

}