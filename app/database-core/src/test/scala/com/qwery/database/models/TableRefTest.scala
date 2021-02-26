package com.qwery.database.models

import com.qwery.database.util.JSONSupport.JSONProductConversion
import com.qwery.models.TableRef
import org.scalatest.funspec.AnyFunSpec
import ModelsJsonProtocol._

/**
  * Table Reference Test Suite
  */
class TableRefTest extends AnyFunSpec {
  val databaseName = "finance"
  val schemaName = "mortgage"
  val tableName = "securities"

  describe(classOf[TableRef].getSimpleName) {
    val table = new TableRef(databaseName, schemaName, tableName)

    it("should render itself as JSON") {
      val jsString = table.toJSON
      info(jsString)
      assert(jsString == s"""{"databaseName":"$databaseName","schemaName":"$schemaName","tableName":"$tableName"}""")
    }

    it("should render itself as SQL") {
      val sql = table.toSQL
      info(sql)
      assert(sql == s"$databaseName.$schemaName.$tableName")
    }

    it("parse/toSQL should be reciprocal") {
      assert(table == TableRef.parse(table.toSQL))
    }

  }

}
