package com.qwery.database

import com.qwery.database.ColumnTypes._
import com.qwery.database.ItemConversionTest.PixAllData
import com.qwery.database.PersistentSeq.Field
import org.scalatest.funspec.AnyFunSpec

/**
 * Product Helper Test Suite
 */
class ItemConversionTest extends AnyFunSpec {
  private val productConversion = PersistentSeq[PixAllData]()

  describe(classOf[ItemConversion[PixAllData]].getSimpleName) {

    it("should extract values from a product class") {
      val data = PixAllData(idValue = "Hello", idType = "World", responseTime = 307, reportDate = 1592204400000L, _id = 1087L)
      val values = productConversion.toKeyValues(data)
      assert(values == Seq(
        "idValue" -> Some("Hello"),
        "idType" -> Some("World"),
        "responseTime" -> Some(307),
        "reportDate" -> Some(1592204400000L),
        "_id" -> Some(1087L)
      ))
    }

    it("should populate a product class with vales") {
      val fmd = FieldMetaData(isCompressed = false, isEncrypted = false, isNotNull = true, `type` = StringType)
      val data = productConversion.createItem(Seq(
        Field(name = "_id", fmd.copy(`type` = LongType), value = Some(1087L)),
        Field(name = "idValue", fmd, Some("Hello")),
        Field(name = "idType", fmd, Some("World")),
        Field(name = "responseTime", fmd.copy(`type` = IntType), Some(307)),
        Field(name = "reportDate", fmd.copy(`type` = LongType), Some(java.sql.Date.valueOf("2020-06-15").getTime))
      ))
      assert(data == PixAllData(idValue = "Hello", idType = "World", responseTime = 307, reportDate = 1592204400000L, _id = 1087))
    }

  }

}

/**
 * Product Helper Test Companion
 */
object ItemConversionTest {

  import scala.annotation.meta.field

  case class PixAllData(@(ColumnInfo@field)(maxSize = 5) idValue: String,
                        @(ColumnInfo@field)(maxSize = 5) idType: String,
                        responseTime: Int,
                        reportDate: Long,
                        _id: Long = 0L)

}
