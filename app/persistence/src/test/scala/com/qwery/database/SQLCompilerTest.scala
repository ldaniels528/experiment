package com.qwery.database

import com.qwery.database.QueryProcessor.commands.{InsertRows, SelectRows}
import com.qwery.models.{expressions => m}
import org.scalatest.funspec.AnyFunSpec

/**
 * SQL Compiler Test Suite
 */
class SQLCompilerTest extends AnyFunSpec {

  describe(classOf[SQLCompilerTest].getSimpleName) {

    it("should compile an INSERT statement") {
      val result = SQLCompiler.compile(databaseName = "test", sql =
        """|INSERT OVERWRITE TABLE OilGasSecurities (Symbol, Name, Sector, Industry, LastSale)
           |VALUES ('AAPL', 'Apple, Inc.', 'Technology', 'Computers', 203.45),
           |       ('AMD', 'American Micro Devices, Inc.', 'Technology', 'Computers', 22.33)
           |""".stripMargin)
      assert(result == InsertRows(databaseName = "test", tableName = "OilGasSecurities",
        columns = List("Symbol", "Name", "Sector", "Industry", "LastSale"),
        values = List(
          List("AAPL", "Apple, Inc.", "Technology", "Computers", 203.45),
          List("AMD", "American Micro Devices, Inc.", "Technology", "Computers", 22.33))
      ))
    }

    it("should compile a SELECT statement") {
      val result = SQLCompiler.compile(databaseName = "test", sql =
        """|SELECT Symbol, Name, Sector, Industry
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |LIMIT 100
           |""".stripMargin)
      assert(result == SelectRows(databaseName = "test", tableName = "Customers",
        fields = List(m.Field("Symbol"), m.Field("Name"), m.Field("Sector"), m.Field("Industry")),
        where = RowTuple("Industry" -> "Oil/Gas Transmission"),
        limit = Some(100))
      )
    }

  }

}
