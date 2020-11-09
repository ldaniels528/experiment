package com.qwery.database.device

import java.util.Date

import com.qwery.database.ColumnTypes.{DateType, DoubleType, StringType}
import com.qwery.database.{Column, ColumnMetadata, RowTuple, TableFile}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Column-Oriented File Block Device Test
 */
class ColumnOrientedFileBlockDeviceTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[ColumnOrientedFileBlockDevice].getSimpleName) {

    it("should read/write columnar data") {
      val columns = Seq(
        Column(name = "symbol", metadata = ColumnMetadata(`type` = StringType), maxSize = Some(8)),
        Column(name = "exchange", metadata = ColumnMetadata(`type` = StringType), maxSize = Some(8)),
        Column(name = "lastSale", metadata = ColumnMetadata(`type` = DoubleType)),
        Column(name = "lastSaleTime", metadata = ColumnMetadata(`type` = DateType))
      )

      // get a reference to the file
      val file = TableFile.getTableDataFile("test", "stocks_columns")
      file.getParentFile.mkdirs()

      // create a column-oriented file device
      ColumnOrientedFileBlockDevice(columns, file) use { device =>
        // truncate the file
        device.shrinkTo(0)

        // write a record to the table
        val buf0 = device.toRowBuffer(RowTuple("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 99.98, "lastSaleTime" -> new Date))
        device.writeRow(rowID = 0, buf0)

        // write a record to the table
        val buf1 = device.toRowBuffer(RowTuple("symbol" -> "GE", "exchange" -> "NYSE", "lastSale" -> 56.45, "lastSaleTime" -> new Date))
        device.writeRow(rowID = 1, buf1)

        // retrieve the record from the table
        val buf2 = device.readRow(1)
        assert(buf1.array() sameElements buf2.array())

        // show the fields
        device.columns zip device.toFields(buf1) foreach { case (column, field) =>
          logger.info(s"${column.name}: ${field.typedValue.toString}")
        }
      }
    }

  }

}
