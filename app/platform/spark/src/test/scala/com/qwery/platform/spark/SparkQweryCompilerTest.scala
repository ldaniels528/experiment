package com.qwery.platform.spark

import com.qwery.models.Insert.{Overwrite, Values}
import com.qwery.models._
import com.qwery.models.expressions._
import org.scalatest.FunSpec

/**
  * Spark Qwery Compiler Test Suite
  * @author lawrence.daniels@gmail.com
  */
class SparkQweryCompilerTest extends FunSpec {
  private val compiler = new SparkQweryCompiler {}

  describe(classOf[SparkQweryCompiler].getSimpleName) {
    import com.qwery.models.expressions.Expression.Implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("should support CREATE FUNCTION statements") {
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
            Column(name = "Industry", `type` = ColumnTypes.STRING)),
          fieldDelimiter = ",",
          headersIncluded = true,
          nullValue = "n/a",
          inputFormat = StorageFormats.CSV,
          location = "./samples/companylist/csv/"
        )),

        // create the function
        Create(UserDefinedFunction(name = "currency", `class` = "com.github.ldaniels528.qwery.Currency", jar = None)),

        // project/transform the data
        Select(
          fields = List(
            Field(descriptor = "Symbol"),
            Field(descriptor = "Name"),
            Field(descriptor = "LastSale"),
            FunctionCall(name = "currency", List(Field(descriptor = "MarketCap"))),
            Field(descriptor = "IPOyear"),
            Field(descriptor = "Sector"),
            Field(descriptor = "Industry")),
          from = Table("Securities"),
          where = IsNotNull(Field("Symbol")) && IsNotNull(Field("Sector")),
          orderBy = List(OrderColumn(name = "Symbol"))
        ))

      implicit val rc: SparkQweryContext = new SparkQweryContext()
      val df = compiler.compile(sql).execute(input = None)
      df.foreach(_.show(20))
    }

    it("should support CREATE LOGICAL TABLE statements") {
      val sql = SQL(
        Create(LogicalTable(
          name = "SpecialSecurities",
          columns = List("symbol STRING", "lastSale DOUBLE").map(Column.apply),
          source = Insert.Values(List(List("AAPL", 202.11), List("AMD", 23.50), List("GOOG", 765.33), List("AMZN", 699.01))))),
        Select(fields = List(AllFields), from = Table("SpecialSecurities"))
      )

      implicit val rc: SparkQweryContext = new SparkQweryContext()
      val df = compiler.compile(sql).execute(input = None)
      df.foreach(_.show(5))
    }

    it("should support CREATE VIEW statements") {
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
            Column(name = "SummaryQuote", `type` = ColumnTypes.STRING)),
          inputFormat = StorageFormats.JSON,
          nullValue = "n/a",
          location = "./samples/companylist/json/"
        )),

        // create a view on the table
        Create(View(name = "OilGasTransmissions",
          Select(
            fields = Seq('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry, 'SummaryQuote).map(s => Field(s.name)),
            from = Table("Securities"),
            where = Field("Industry") === "Oil/Gas Transmission" && IsNotNull(Field("IPOyear"))
          ))),

        // select the records via the view
        Select(fields = Seq(AllFields), from = Table("OilGasTransmissions"))
      )

      implicit val rc: SparkQweryContext = new SparkQweryContext()
      val df = compiler.compile(sql).execute(input = None)
      df.foreach(_.show(5))
    }

    it("should support SELECT w/ORDER BY & LIMIT statements") {
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
          fieldDelimiter = ",",
          headersIncluded = true,
          nullValue = "n/a",
          inputFormat = StorageFormats.CSV,
          location = "./samples/companylist/csv/"
        )),

        // project/transform the data
        Select(
          fields = List(
            Field(descriptor = "Symbol"),
            Field(descriptor = "Name"),
            Field(descriptor = "LastSale"),
            Field(descriptor = "MarketCap"),
            Field(descriptor = "IPOyear"),
            Field(descriptor = "Sector"),
            Field(descriptor = "Industry")),
          from = Table("Securities"),
          where = IsNotNull(Field("Symbol")) && IsNotNull(Field("Sector")),
          orderBy = List(OrderColumn(name = "Symbol")),
          limit = 5
        ))

      implicit val rc: SparkQweryContext = new SparkQweryContext()
      val df = compiler.compile(sql).execute(input = None)
      df.foreach(_.show(5))
    }

    it("should support SELECT w/GROUP BY & ORDER BY statements") {
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
          fieldDelimiter = ",",
          headersIncluded = true,
          nullValue = "n/a",
          inputFormat = StorageFormats.CSV,
          location = "./samples/companylist/csv/"
        )),

        // project/transform the data
        Select(
          fields = List(
            Field(descriptor = "Sector"),
            Field(descriptor = "Industry"),
            Field(name = "DeptCode", value = "1337"),
            Avg(Field(descriptor = "LastSale")).as("AvgLastSale"),
            Max(Field(descriptor = "LastSale")).as("MaxLastSale"),
            Min(Field(descriptor = "LastSale")).as("MinLastSale"),
            Count(AllFields).as("Companies")),
          from = Table("Securities"),
          where = Field("Sector") === "Basic Industries",
          groupBy = List("Sector", "Industry"),
          orderBy = List(OrderColumn(name = "Sector"), OrderColumn(name = "Industry"))
        ))

      implicit val rc: SparkQweryContext = new SparkQweryContext()
      val df = compiler.compile(sql).execute(input = None)
      df.foreach(_.show(5))
    }

    it("should support INSERT-VALUES statements") {
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
          fieldDelimiter = ",",
          headersIncluded = true,
          nullValue = "n/a",
          inputFormat = StorageFormats.CSV,
          location = "./samples/companylist/csv/"
        )),

        // create the output table
        Create(Table(name = "OilGasSecurities",
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
          fieldDelimiter = ",",
          headersIncluded = true,
          nullValue = "n/a",
          outputFormat = StorageFormats.JSON,
          location = "./temp/json/"
        )),

        // select the records via the view
        {
          val fields = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry, 'SummaryQuote, 'Reserved).map(s => Field(s.name))
          Insert(Overwrite(Table("OilGasSecurities")),
            Values(values = List(
              List("AAPL", "Apple Inc.", 215.49, "$1040.8B", "1980", "Technology", "Computer Manufacturing", "https://www.nasdaq.com/symbol/aapl", "n/a")
            )),
            fields = fields)
        }
      )

      implicit val rc: SparkQweryContext = new SparkQweryContext()
      val df = compiler.compile(sql).execute(input = None)
      df.foreach(_.show(5))
    }

    it("should support INSERT-SELECT statements") {
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
          nullValue = "n/a",
          inputFormat = StorageFormats.JSON,
          location = "./samples/companylist/json/"
        )),

        // create the output table
        Create(Table(name = "OilGasSecurities",
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
          fieldDelimiter = ",",
          headersIncluded = true,
          nullValue = "n/a",
          outputFormat = StorageFormats.CSV,
          location = "./temp/oil-gas/"
        )),

        // select the records via the view
        {
          val fields = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry, 'SummaryQuote, 'Reserved).map(s => Field(s.name))
          Insert(Overwrite(Table("OilGasSecurities")),
            Select(
              fields = fields,
              from = Table("Securities"),
              where = Field("Industry") === "Oil/Gas Transmission"),
            fields = fields)
        })

      implicit val rc: SparkQweryContext = new SparkQweryContext()
      val df = compiler.compile(sql).execute(input = None)
      df.foreach(_.show(5))
    }

  }

}
