package com.qwery.database.device

import java.util.Date

import com.qwery.database.ColumnTypes.{DateType, DoubleType, StringType}
import com.qwery.database.DatabaseFiles._
import com.qwery.database.{Column, ColumnMetadata, KeyValues, Row}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Row-Oriented File Block Device Test
 */
class RowOrientedFileBlockDeviceTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[RowOrientedFileBlockDevice].getSimpleName) {

    it("should read/write row data") {
      val columns = Seq(
        Column(name = "symbol", metadata = ColumnMetadata(`type` = StringType), maxSize = Some(8)),
        Column(name = "exchange", metadata = ColumnMetadata(`type` = StringType), maxSize = Some(8)),
        Column(name = "lastSale", metadata = ColumnMetadata(`type` = DoubleType)),
        Column(name = "lastSaleTime", metadata = ColumnMetadata(`type` = DateType))
      )

      // get a reference to the file
      val file = getTableDataFile("test", "stocks_rows")
      file.getParentFile.mkdir()

      // create a row-oriented file device
      new RowOrientedFileBlockDevice(columns, file) use { implicit device =>
        // truncate the file
        device.shrinkTo(0)

        // write a record to the table
        val buf0 = KeyValues("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 99.98, "lastSaleTime" -> new Date).toRowBuffer
        device.writeRowAsBinary(rowID = 0, buf0)

        // retrieve the record from the table
        val buf1 = device.readRowAsBinary(0)

        // show the fields
        val fields = Row.toFields(buf1)(device)
        fields foreach { field =>
          logger.info(field.typedValue.toString)
        }

      }
    }

  }

}
