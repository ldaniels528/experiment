package com.qwery.database.models

import com.qwery.database.models.ModelsJsonProtocol._
import com.qwery.models.TableRef
import spray.json._

/**
  * Represents a reference to a Table Index
  * @param ref             the [[TableRef table reference]]
  * @param indexColumnName the name of the index column
  */
case class TableIndexRef(ref: TableRef, indexColumnName: String)

/**
  * Table Index Reference Companion
  */
object TableIndexRef {

  implicit val tableIndexJsonFormat: RootJsonFormat[TableIndexRef] = jsonFormat2(TableIndexRef.apply)

}