package com.qwery.database.server

import java.io.File

import com.qwery.database.server.TableFile.getDataDirectory
import com.qwery.database.server.TableManager.{InvokableFacade, Result}
import com.qwery.language.SQLLanguageParser
import com.qwery.models.expressions.{BasicField, ConditionalOp, Literal}
import com.qwery.models._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
 * Table Manager
 */
class TableManager() {
  private val tables = TrieMap[String, TableFile]()

  def apply(tableName: String): TableFile = tables.getOrElseUpdate(tableName, TableFile(tableName))

  def drop(tableName: String): Unit = ??? // TODO add drop table logic

  def query(sql: String): Seq[Result] = SQLLanguageParser.parse(sql).translate(this)

  private def findTables(theFile: File = getDataDirectory): List[File] = theFile match {
    case directory if directory.isDirectory => directory.listFiles().toList.flatMap(findTables)
    case file if file.getName.endsWith(".qdb") => file :: Nil
    case _ => Nil
  }

}

/**
 * Table Manager Companion
 */
object TableManager {
  private val logger = LoggerFactory.getLogger(getClass)

  type Result = Seq[(String, Any)]

  def invoke(select: Select)(implicit tableManager: TableManager): Seq[Result] = {
    select.from match {
      case Some(TableRef(tableName)) =>
        val table = tableManager.apply(tableName)
        val conditions: Map[Symbol, Any] = select.where match {
          case Some(ConditionalOp(BasicField(name), Literal(value), "==", "=")) =>
            Map(Symbol(name) -> value)
          case Some(condition) =>
            throw new IllegalArgumentException(s"Unhandled condition $condition")
          case None => Map.empty
        }
        table.find(conditions, limit = select.limit)  // TODO select.fields & select.orderBy
      case Some(queryable) =>
        logger.info(s"queryable = $queryable")
        ???
      case None => ???
    }
  }

  def invoke(truncate: Truncate)(implicit tableManager: TableManager): Seq[Result] = {
    truncate.table match {
      case TableRef(name) => tableManager(name).truncate()
      case unknown => throw new IllegalArgumentException(s"Unhandled table source $unknown")
    }
    Nil
  }

  final implicit class InvokableFacade(val invokable: Invokable) extends AnyVal {
    def translate(implicit tableManager: TableManager): Seq[Result] = invokable match {
      case select: Select => invoke(select)
      case truncate: Truncate => invoke(truncate)
      case unknown => throw new IllegalArgumentException(s"Unhandled operation $unknown")
    }
  }

}