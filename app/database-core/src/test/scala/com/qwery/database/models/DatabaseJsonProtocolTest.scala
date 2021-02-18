package com.qwery.database
package models

import com.qwery.database.JSONSupport._
import com.qwery.database.files.TableConfig
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import spray.json._

/**
 * Database JSON Protocol Test
 */
class DatabaseJsonProtocolTest extends AnyFunSpec {

  describe(DatabaseJsonProtocol.getClass.getSimpleName) {

    it("should serialize/deserialize a Column") {
      verifyJson(Column(name = "symbol", comment = "stock ticker", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), enumValues = Nil, sizeInBytes = 24))
    }

    it("should serialize/deserialize a TableConfig") {
      val tableConfig = TableConfig(description = Some("test config") , columns = Seq(
        Column(name = "symbol", comment = "stock ticker", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), enumValues = Nil, sizeInBytes = 24)
      ))
      verifyJson(tableConfig)
    }

  }

  def verifyJson[A](entity0: A)(implicit reader: JsonReader[A], writer: JsonWriter[A]): Assertion = {
    val entity0Js = entity0.toJSON
    info(entity0Js)
    val entity1 = entity0Js.fromJSON[A]
    assert(entity0 == entity1)
  }

}
