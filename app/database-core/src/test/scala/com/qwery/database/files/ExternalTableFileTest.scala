package com.qwery.database
package files

import com.qwery.database.models.KeyValues
import com.qwery.language.SQLDecompiler.implicits._
import com.qwery.models.{Column, ColumnTypeSpec, EntityRef, ExternalTable}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

import scala.util.Try

/**
  * External Table File Test
  */
class ExternalTableFileTest extends AnyFunSpec {
  val databaseName = "test"
  val schemaName = "public"
  val tableName = "companyList"
  val tableRef = new EntityRef(databaseName, schemaName, tableName)

  describe(classOf[ExternalTableFile].getName) {

    it("should create an external table") {
      val table = ExternalTableFile.createTable(
        table = ExternalTable(
          ref = tableRef,
          location = Some("./samples/companylist/csv/"),
          inputFormat = Some("csv"),
          nullValue = Some("n/a"),
          columns = List(
            Column(name = "Symbol", spec = new ColumnTypeSpec(`type` = "String", size = 10)),
            Column(name = "Name", spec = new ColumnTypeSpec(`type` = "String", size = 70)),
            Column(name = "LastSale", spec = new ColumnTypeSpec(`type` = "String", size = 12)),
            Column(name = "MarketCap", spec = new ColumnTypeSpec(`type` = "String", size = 20)),
            Column(name = "IPOyear", spec = new ColumnTypeSpec(`type` = "String", size = 8)),
            Column(name = "Sector", spec = new ColumnTypeSpec(`type` = "String", size = 70)),
            Column(name = "Industry", spec = new ColumnTypeSpec(`type` = "String", size = 70)),
            Column(name = "SummaryQuote", spec = new ColumnTypeSpec(`type` = "String", size = 50)),
            Column(name = "Reserved", spec = new ColumnTypeSpec(`type` = "String", size = 20))
          )))
      table.close()
    }

    it("should fetch a specific row") {
      val rowID: ROWID = 20
      ExternalTableFile(tableRef) use { table =>
        val row = table.getRow(rowID)
        info(s"row($rowID) => ${row.map(_.toKeyValues)}")
        assert(row.map(_.toKeyValues.toMap) contains Map(
          "__id" -> rowID, "Sector" -> "Health Care", "Name" -> "American Shared Hospital Services",
          "SummaryQuote" -> "https://www.nasdaq.com/symbol/ams", "Industry" -> "Medical Specialities",
          "Symbol" -> "AMS", "LastSale" -> "3.05", "MarketCap" -> "$17.43M"
        ))
      }
    }

    it("should not allow modifications") {
      ExternalTableFile(tableRef) use { table =>
        val outcome = Try(table.insertRow(KeyValues(
          "Sector" -> "Health Care", "Name" -> "American Shared Hospital Services",
          "SummaryQuote" -> "https://www.nasdaq.com/symbol/ams", "Industry" -> "Medical Specialities",
          "Symbol" -> "AMS", "IPOyear" -> "n/a", "LastSale" -> "3.05", "MarketCap" -> "$17.43M"
        )))
        info(s"outcome: $outcome")
        assert(outcome.isFailure && outcome.failed.get.getMessage == s"Table '${tableRef.toSQL}' is read-only")
      }
    }

    it("should determine the length of the device") {
      ExternalTableFile(tableRef) use { table =>
        info(s"externalTableFile.device.length => ${table.device.length}")
        assert(table.device.length == 6878)
      }
    }

  }

}
