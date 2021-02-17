package com.qwery.database
package files

import com.qwery.models.{ColumnSpec, ExternalTable, StorageFormats}
import com.qwery.util.ResourceHelper._
import com.qwery.{models => mx}
import org.scalatest.funspec.AnyFunSpec

import scala.util.Try

/**
  * External Table File Test
  */
class ExternalTableFileTest extends AnyFunSpec {
  val databaseName = "test"
  val tableName = "companyList"

  describe(classOf[ExternalTableFile].getName) {

    it("should create an external table") {
      val table = ExternalTableFile.createTable(databaseName,
        ref = ExternalTable(tableName,
          location = Some("./samples/companylist/csv/"),
          format = Some(StorageFormats.CSV),
          nullValue = Some("n/a"),
          columns = List(
            mx.Column(name = "Symbol", spec = ColumnSpec(typeName = "String", precision = List(10))),
            mx.Column(name = "Name", spec = ColumnSpec(typeName = "String", precision = List(70))),
            mx.Column(name = "LastSale", spec = ColumnSpec(typeName = "String", precision = List(12))),
            mx.Column(name = "MarketCap", spec = ColumnSpec(typeName = "String", precision = List(20))),
            mx.Column(name = "IPOyear", spec = ColumnSpec(typeName = "String", precision = List(8))),
            mx.Column(name = "Sector", spec = ColumnSpec(typeName = "String", precision = List(70))),
            mx.Column(name = "Industry", spec = ColumnSpec(typeName = "String", precision = List(70))),
            mx.Column(name = "SummaryQuote", spec = ColumnSpec(typeName = "String", precision = List(50))),
            mx.Column(name = "Reserved", spec = ColumnSpec(typeName = "String", precision = List(20)))
          )))
      table.close()
    }

    it("should fetch a specific row") {
      val rowID: ROWID = 20
      ExternalTableFile(databaseName, tableName) use { table =>
        val row = table.getRow(rowID)
        info(s"row($rowID) => ${row.map(_.toKeyValues)}")
        assert(row.map(_.toKeyValues.toMap) contains Map(
          "__id" -> rowID, "Sector" -> "Health Care", "Name" -> "American Shared Hospital Services",
          "SummaryQuote" -> "https://www.nasdaq.com/symbol/ams", "Industry" -> "Medical Specialities",
          "Symbol" -> "AMS", "IPOyear" -> "n/a", "LastSale" -> "3.05", "MarketCap" -> "$17.43M"
        ))
      }
    }

    it("should not allow modifications") {
      ExternalTableFile(databaseName, tableName) use { table =>
        val outcome = Try(table.insertRow(KeyValues(
          "Sector" -> "Health Care", "Name" -> "American Shared Hospital Services",
          "SummaryQuote" -> "https://www.nasdaq.com/symbol/ams", "Industry" -> "Medical Specialities",
          "Symbol" -> "AMS", "IPOyear" -> "n/a", "LastSale" -> "3.05", "MarketCap" -> "$17.43M"
        )))
        info(s"outcome: $outcome")
        assert(outcome.isFailure && outcome.failed.get.getMessage == "Table test.companyList is read-only")
      }
    }

    it("should determine the length of the device") {
      ExternalTableFile(databaseName, tableName) use { table =>
        info(s"externalTableFile.device.length => ${table.device.length}")
        assert(table.device.length == 6878)
      }
    }

  }

}