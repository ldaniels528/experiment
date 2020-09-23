package com.qwery.database.server

import com.qwery.database.PersistentSeq.newTempFile
import com.qwery.database.{BlockDevice, FileBlockDevice}
import com.qwery.language._
import com.qwery.models.Invokable

/**
 * Represents a database table
 * @param device the [[BlockDevice]]
 */
case class TableFile(device: BlockDevice) {

  def sql(query: String): Invokable = {
    SQLLanguageParser.parse(query)
  }

}

object TableFile {

  def apply(name: String): TableFile = {
    val file = newTempFile()
    val device = new FileBlockDevice(columns = ???, file)
    val table = TableFile(device)
    table
  }

}