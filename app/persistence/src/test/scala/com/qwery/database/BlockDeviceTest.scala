package com.qwery.database

import com.qwery.database.PersistentSeq.{Field, newTempFile, toColumns}
import org.scalatest.funspec.AnyFunSpec

class BlockDeviceTest extends AnyFunSpec {
  private val stocks = Array(
    StockQuote(symbol = "BXXG", exchange = "NASDAQ", lastSale = 147.63, lastSaleTime = 1596317591000L),
    StockQuote(symbol = "KFFQ", exchange = "NYSE", lastSale = 22.92, lastSaleTime = 1597181591000L),
    StockQuote(symbol = "GTKK", exchange = "NASDAQ", lastSale = 240.14, lastSaleTime = 1596835991000L),
    StockQuote(symbol = "KNOW", exchange = "OTCBB", lastSale = 357.21, lastSaleTime = 1597872791000L)
  )
  val (columns, _) = toColumns[StockQuote]
  private val persistenceFile = newTempFile()
  private val coll = PersistentSeq.builder[StockQuote].withPersistenceFile(persistenceFile).build

  describe(classOf[BlockDevice].getSimpleName) {

    it("test reading a column") {
      coll ++= stocks
      println(s"coll contains ${coll.length} items")
      coll.zipWithIndex.foreach { case (item, index) => println(f"[${index + 1}] $item") }
      println()

      // read the symbol from the 1st record
      assert(grabField(rowID = 0, columnIndex = 0).value.contains("BXXG"))

      // read the exchange from the 2nd record
      assert(grabField(rowID = 1, column = 'exchange).value.contains("NYSE"))

      // read the lastSale from the 3rd record
      assert(grabField(rowID = 2, column = 'lastSale).value.contains(240.14))

      // read the lastSaleTime from the 4th record
      assert(grabField(rowID = 3, columnIndex = 3).value.contains(1597872791000L))
    }

  }

  def grabField(rowID: ROWID, columnIndex: Int): Field = {
    val field@Field(name, fmd, value_?) = coll.device.getField(rowID, columnIndex)
    println(s"$name: ${value_?} -> $fmd")
    field
  }

  def grabField(rowID: ROWID, column: Symbol): Field = {
    val field@Field(name, fmd, value_?) = coll.device.getField(rowID, column)
    println(s"$name: ${value_?} -> $fmd")
    field
  }

}
