package com.qwery.database.server

import java.io.File

import com.qwery.database.server.TableFile.getDataDirectory
import com.qwery.database.server.TableQueryExecutor.{InvokableFacade, Result}
import com.qwery.language.SQLLanguageParser

import scala.collection.concurrent.TrieMap

/**
 * Table Manager
 */
class TableManager() {
  private val tables = TrieMap[String, TableFile]()

  def get(tableName: String): TableFile = tables.getOrElseUpdate(tableName, TableFile(tableName))

  def query(sql: String): Seq[Result] = SQLLanguageParser.parse(sql).translate(this)

  private def findTables(theFile: File = getDataDirectory): List[File] = theFile match {
    case directory if directory.isDirectory => directory.listFiles().toList.flatMap(findTables)
    case file if file.getName.endsWith(".bin") => file :: Nil
    case _ => Nil
  }

}
