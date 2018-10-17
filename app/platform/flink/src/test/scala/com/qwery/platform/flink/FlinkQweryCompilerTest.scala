package com.qwery.platform.flink

import com.qwery.models._
import org.scalatest.FunSpec

import scala.language.postfixOps

/**
  * Flink Qwery Compiler Test Suite
  * @author lawrence.daniels@gmail.com
  */
class FlinkQweryCompilerTest extends FunSpec {
  private val compiler = new FlinkQweryCompiler {}

  describe(classOf[FlinkQweryCompiler].getSimpleName) {
    import com.qwery.models.expressions.implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("should compile a SELECT w/ORDER BY & LIMIT") {
      val sql = SQL(
        // create the input table
        Create(Table(name = "Securities",
          columns = List(
            Column(name = "Symbol", `type` = ColumnTypes.STRING),
            Column(name = "Name", `type` = ColumnTypes.STRING),
            Column(name = "LastSale", `type` = ColumnTypes.DOUBLE),
            Column(name = "MarketCap", `type` = ColumnTypes.STRING),
            Column(name = "IPOyear", `type` = ColumnTypes.STRING),
            Column(name = "Sector", `type` = ColumnTypes.STRING),
            Column(name = "Industry", `type` = ColumnTypes.STRING),
            Column(name = "SummaryQuote", `type` = ColumnTypes.STRING),
            Column(name = "Reserved", `type` = ColumnTypes.STRING)),
          inputFormat = StorageFormats.CSV,
          outputFormat = StorageFormats.CSV,
          location = "./samples/companylist/csv/"
        )),

        // project/transform the data
        Select(
          fields = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry),
          from = Table("Securities"),
          orderBy = List('Symbol asc)
        ))

      implicit val rc: FlinkQweryContext = new FlinkQweryContext()
      val operation = compiler.compile(sql)
      operation.execute(input = None)
    }

  }

}
