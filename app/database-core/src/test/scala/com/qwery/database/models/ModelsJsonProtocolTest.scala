package com.qwery.database
package models

import com.qwery.database.models.ModelsJsonProtocol._
import com.qwery.database.util.JSONSupport._
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import spray.json._

/**
 * Models JSON Protocol Test
 */
class ModelsJsonProtocolTest extends AnyFunSpec {

  describe(ModelsJsonProtocol.getClass.getSimpleName) {

    it("should serialize/deserialize a Column") {
      verifyJson(Column(name = "symbol", comment = "stock ticker", metadata = models.ColumnMetadata(`type` = ColumnTypes.StringType), enumValues = Nil, sizeInBytes = 24))
    }

    it("should serialize/deserialize a ColumnMetadata") {
      verifyJson(models.ColumnMetadata(`type` = ColumnTypes.StringType))
    }

    it("should serialize/deserialize a ColumnSearchResult") {
      verifyJson(ColumnSearchResult(databaseName = "test", schemaName = "school", tableName = "students", column = Column(name = "symbol", comment = "stock ticker", metadata = models.ColumnMetadata(`type` = ColumnTypes.StringType), enumValues = Nil, sizeInBytes = 24)))
    }

    it("should serialize/deserialize a DatabaseSearchResult") {
      verifyJson(DatabaseSearchResult(databaseName = "test"))
    }

    it("should serialize/deserialize a TableConfig") {
      verifyJson(TableConfig(description = Some("test config") , columns = Seq(
        Column(name = "symbol", comment = "stock ticker", metadata = models.ColumnMetadata(`type` = ColumnTypes.StringType), enumValues = Nil, sizeInBytes = 24)
      )))
    }

    it("should serialize/deserialize a TableSearchResult") {
      verifyJson(TableSearchResult(databaseName = "test", schemaName = "school", tableName = "students", tableType = "TABLE", description = Some("student database")))
    }

  }

  def verifyJson[A](entity0: A)(implicit reader: JsonReader[A], writer: JsonWriter[A]): Assertion = {
    val entity0Js = entity0.toJSON
    info(entity0Js)
    val entity1 = entity0Js.fromJSON[A]
    assert(entity0 == entity1)
  }

}
