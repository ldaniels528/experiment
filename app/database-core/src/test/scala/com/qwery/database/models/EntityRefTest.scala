package com.qwery.database.models

import com.qwery.database.models.ModelsJsonProtocol._
import com.qwery.database.util.JSONSupport.JSONProductConversion
import com.qwery.language.SQLDecompiler.implicits._
import com.qwery.models.EntityRef
import org.scalatest.funspec.AnyFunSpec

/**
  * Table Reference Test Suite
  */
class EntityRefTest extends AnyFunSpec {
  val databaseName = "finance"
  val schemaName = "mortgage"
  val tableName = "securities"

  describe(classOf[EntityRef].getSimpleName) {
    val table = new EntityRef(databaseName, schemaName, tableName)

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
      assert(table == EntityRef.parse(table.toSQL))
    }

  }

}
