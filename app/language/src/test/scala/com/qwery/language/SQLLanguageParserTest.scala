package com.qwery.language

import com.qwery.models.Console.Print
import com.qwery.models.Insert.{Into, Overwrite}
import com.qwery.models._
import com.qwery.models.expressions._
import org.scalatest.FunSpec

import scala.language.postfixOps

/**
  * SQL Language Parser Test
  * @author lawrence.daniels@gmail.com
  */
class SQLLanguageParserTest extends FunSpec {

  describe(classOf[SQLLanguageParser].getSimpleName) {
    import com.qwery.models.expressions.implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("should support BEGIN ... END statements") {
      // test type 1
      val results1 = SQLLanguageParser.parse(
        """|BEGIN
           |  PRINT 'Hello World'
           |END
           |""".stripMargin)
      assert(results1 == Console.Print("Hello World"))

      // test type 2
      val results2 = SQLLanguageParser.parse(
        """|{
           |  PRINT 'Hello World'
           |}
           |""".stripMargin)
      assert(results2 == Console.Print("Hello World"))
    }

    it("should support CALL statements") {
      val results = SQLLanguageParser.parse("CALL computeArea(length, width)")
      assert(results == ProcedureCall(name = "computeArea", args = List('length, 'width)))
    }

    it("should support CREATE FUNCTION statements") {
      val results = SQLLanguageParser.parse("CREATE FUNCTION myFunc AS 'com.qwery.udf.MyFunc'")
      assert(results == Create(UserDefinedFunction(name = "myFunc", `class` = "com.qwery.udf.MyFunc", jarLocation = None)))
    }

    it("should support CREATE INLINE TABLE statements") {
      val results = SQLLanguageParser.parse(
        """|CREATE INLINE TABLE SpecialSecurities (symbol STRING, lastSale DOUBLE)
           |FROM VALUES ('AAPL', 202.11), ('AMD', 23.50), ('GOOG', 765.33), ('AMZN', 699.01)
           |""".stripMargin)
      assert(results == Create(InlineTable(
        name = "SpecialSecurities",
        columns = List("symbol STRING", "lastSale DOUBLE").map(Column.apply),
        source = Insert.Values(List(List("AAPL", 202.11), List("AMD", 23.50), List("GOOG", 765.33), List("AMZN", 699.01)))
      )))
    }

    it("should support CREATE PROCEDURE statements") {
      val results = SQLLanguageParser.parse(
        """|CREATE PROCEDURE testInserts(industry STRING) AS
           |  RETURN (
           |    SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |    FROM Customers
           |    WHERE Industry = @industry
           |  )
           |""".stripMargin)
      assert(results == Create(Procedure(name = "testInserts",
        params = List("industry STRING").map(Column.apply),
        code = Return(Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = Field('Industry) === @@(name = "industry")
        ))
      )))
    }

    it("should support CREATE EXTERNAL TABLE statements") {
      val results1 = SQLLanguageParser.parse(
        """|CREATE EXTERNAL TABLE Customers (customer_id STRING, name STRING, address STRING, ingestion_date LONG)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |STORED AS INPUTFORMAT 'PARQUET' OUTPUTFORMAT 'PARQUET'
           |LOCATION './dataSets/customers/parquet/'
           |""".stripMargin)
      assert(results1 == Create(Table(name = "Customers",
        columns = List("customer_id STRING", "name STRING", "address STRING", "ingestion_date LONG").map(Column.apply),
        partitionBy = List("year STRING", "month STRING", "day STRING").map(Column.apply),
        inputFormat = StorageFormats.PARQUET,
        outputFormat = StorageFormats.PARQUET,
        location = LocationRef("./dataSets/customers/parquet/")
      )))
    }

    it("should support CREATE TABLE statements") {
      val results2 = SQLLanguageParser.parse(
        """|CREATE TABLE Customers (customer_uid UUID, name STRING, address STRING, ingestion_date LONG)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |STORED AS INPUTFORMAT 'JSON' OUTPUTFORMAT 'JSON'
           |LOCATION './dataSets/customers/json/'
           |""".stripMargin)
      assert(results2 == Create(Table(name = "Customers",
        columns = List("customer_uid UUID", "name STRING", "address STRING", "ingestion_date LONG").map(Column.apply),
        partitionBy = List("year STRING", "month STRING", "day STRING").map(Column.apply),
        inputFormat = StorageFormats.JSON,
        outputFormat = StorageFormats.JSON,
        location = LocationRef("./dataSets/customers/json/")
      )))
    }

    it("should support CREATE TABLE w/COMMENT statements") {
      val results = SQLLanguageParser.parse(
        """|CREATE TABLE Customers (
           |    customer_uid UUID COMMENT 'Unique Customer ID',
           |    name STRING COMMENT '',
           |    address STRING COMMENT '',
           |    ingestion_date LONG COMMENT ''
           |)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |ROW FORMAT DELIMITED
           |FIELDS TERMINATED BY ','
           |STORED AS INPUTFORMAT 'CSV'
           |WITH HEADERS ON
           |WITH NULL VALUES AS 'n/a'
           |LOCATION './dataSets/customers/csv/'
           |""".stripMargin)
      assert(results == Create(Table(name = "Customers",
        columns = List(
          Column("customer_uid UUID").withComment("Unique Customer ID"),
          Column("name STRING"), Column("address STRING"), Column("ingestion_date LONG")
        ),
        partitionBy = List("year STRING", "month STRING", "day STRING").map(Column.apply),
        fieldTerminator = ",",
        headersIncluded = true,
        nullValue = Some("n/a"),
        inputFormat = StorageFormats.CSV,
        outputFormat = None,
        location = LocationRef("./dataSets/customers/csv/")
      )))
    }

    it("should support CREATE TABLE ... WITH statements") {
      val results = SQLLanguageParser.parse(
        """|CREATE TABLE Customers (customer_uid UUID, name STRING, address STRING, ingestion_date LONG)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |ROW FORMAT DELIMITED
           |FIELDS TERMINATED BY ','
           |STORED AS INPUTFORMAT 'CSV'
           |WITH HEADERS ON
           |WITH NULL VALUES AS 'n/a'
           |LOCATION './dataSets/customers/csv/'
           |""".stripMargin)
      assert(results == Create(Table(name = "Customers",
        columns = List("customer_uid UUID", "name STRING", "address STRING", "ingestion_date LONG").map(Column.apply),
        partitionBy = List("year STRING", "month STRING", "day STRING").map(Column.apply),
        fieldTerminator = ",",
        headersIncluded = true,
        nullValue = Some("n/a"),
        inputFormat = StorageFormats.CSV,
        outputFormat = None,
        location = LocationRef("./dataSets/customers/csv/")
      )))
    }

    it("should support complex CREATE TABLE statements") {
      val results = SQLLanguageParser.parse(
        """|CREATE EXTERNAL TABLE `kbb_mart.kbb_rev_per_page`(
           |  `rank` string COMMENT 'from deserializer',
           |  `section` string COMMENT 'from deserializer',
           |  `super_section` string COMMENT 'from deserializer',
           |  `page_name` string COMMENT 'from deserializer',
           |  `rev` string COMMENT 'from deserializer',
           |  `last_processed_ts_est` string COMMENT 'from deserializer')
           |PARTITIONED BY (
           |  `hit_dt_est` string,
           |  `site_experience_desc` string)
           |ROW FORMAT SERDE
           |  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
           |WITH SERDEPROPERTIES (
           |  'quoteChar'='\"',
           |  'separatorChar'=',')
           |STORED AS INPUTFORMAT
           |  'org.apache.hadoop.mapred.TextInputFormat'
           |OUTPUTFORMAT
           |  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
           |LOCATION
           |  's3://kbb-etl-mart-dev/kbb_rev_per_page/kbb_rev_per_page'
           |TBLPROPERTIES (
           |  'transient_lastDdlTime'='1555400098')
           |""".stripMargin)
      assert(results == Create(Table(name = "kbb_mart.kbb_rev_per_page",
        columns = List(
          Column("rank string"),
          Column("section string"),
          Column("super_section string"),
          Column("page_name string"),
          Column("rev string"),
          Column("last_processed_ts_est string")
        ).map(_.withComment("from deserializer")),
        partitionBy = List(Column("hit_dt_est string"), Column("site_experience_desc string")),
        inputFormat = StorageFormats.CSV,
        outputFormat = StorageFormats.CSV,
        location = LocationRef("s3://kbb-etl-mart-dev/kbb_rev_per_page/kbb_rev_per_page"),
        serdeProperties = Map("quoteChar" -> "\\\"", "separatorChar" -> ","),
        tableProperties = Map("transient_lastDdlTime" -> "1555400098")
      )))
    }

    it("should support CREATE TEMPORARY FUNCTION") {
      val results = SQLLanguageParser.parse(
        """|CREATE TEMPORARY FUNCTION myFunc AS 'com.qwery.udf.MyFunc'
           |USING JAR '/home/ldaniels/shocktrade/jars/shocktrade-0.8.jar'
           |""".stripMargin)
      assert(results == Create(UserDefinedFunction(
        name = "myFunc",
        `class` = "com.qwery.udf.MyFunc",
        jarLocation = "/home/ldaniels/shocktrade/jars/shocktrade-0.8.jar"
      )))
    }

    it("should support CREATE VIEW statements") {
      val results = SQLLanguageParser.parse(
        """|CREATE VIEW OilAndGas AS
           |SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == Create(View(name = "OilAndGas",
        query = Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = Field('Industry) === "Oil/Gas Transmission"
        ))))
    }

    it("should support DECLARE statements") {
      val results = SQLLanguageParser.parse("DECLARE @customerId INTEGER")
      assert(results == Declare(variable = @@("customerId"), `type` = "INTEGER", isExternal = false))
    }

    it("should support DECLARE EXTERNAL statements") {
      val results = SQLLanguageParser.parse("DECLARE EXTERNAL @customerId INTEGER")
      assert(results == Declare(variable = @@("customerId"), `type` = "INTEGER", isExternal = true))
    }

    it("should support DEBUG, ERROR, INFO, LOG, PRINT and WARN statements") {
      case class Expected(command: String, opCode: String => Console, message: String)
      val tests = Seq(
        Expected("DEBUG", Console.Debug.apply, "This is a debug message"),
        Expected("ERROR", Console.Error.apply, "This is an error message"),
        Expected("INFO", Console.Info.apply, "This is an informational message"),
        Expected("PRINT", Console.Print.apply, "This message will be printed to STDOUT"),
        Expected("WARN", Console.Warn.apply, "This is a warning message"))
      tests foreach { case Expected(command, opCode, message) =>
        val results = SQLLanguageParser.parse(s"$command '$message'")
        assert(results == opCode(message))
      }
    }

    it("should support function call statements") {
      val results = SQLLanguageParser.parse(
        "SELECT symbol, customFx(lastSale) FROM Securities WHERE naics = '12345'")
      assert(results == Select(
        fields = Seq('symbol, FunctionCall("customFx")('lastSale)),
        from = Table("Securities"),
        where = Field('naics) === "12345"
      ))
    }

    it("should support FOR statements") {
      val results = SQLLanguageParser.parse(
        """|FOR #item IN (SELECT symbol, lastSale FROM Securities WHERE naics = '12345') {
           |  PRINT '${item.symbol} is ${item.lastSale)/share';
           |}
           |""".stripMargin)
      assert(results == ForEach(
        variable = @#("item"),
        rows = Select(fields = Seq('symbol, 'lastSale), from = Table("Securities"), where = Field('naics) === "12345"),
        invokable = SQL(
          Print("${item.symbol} is ${item.lastSale)/share")
        ),
        isReverse = false
      ))
    }

    it("should support FOR ... LOOP statements") {
      val results = SQLLanguageParser.parse(
        """|FOR #item IN REVERSE (SELECT symbol, lastSale FROM Securities WHERE naics = '12345')
           |LOOP
           |  PRINT '${item.symbol} is ${item.lastSale)/share';
           |END LOOP;
           |""".stripMargin)
      assert(results == ForEach(
        variable = @#("item"),
        rows = Select(fields = Seq('symbol, 'lastSale), from = Table("Securities"), where = Field('naics) === "12345"),
        invokable = SQL(
          Print("${item.symbol} is ${item.lastSale)/share")
        ),
        isReverse = true
      ))
    }

    it("should support INCLUDE statements") {
      val results = SQLLanguageParser.parse("INCLUDE 'models/securities.sql'")
      assert(results == Include("models/securities.sql"))
    }

    it("should support INSERT statements without explicitly defined fields") {
      val results = SQLLanguageParser.parse(
        "INSERT INTO Students VALUES ('Fred Flintstone', 35, 1.28), ('Barney Rubble', 32, 2.32)")
      assert(results == Insert(Into(Table("Students")), Insert.Values(values = List(
        List("Fred Flintstone", 35.0, 1.28),
        List("Barney Rubble", 32.0, 2.32)
      ))))
    }

    it("should support INSERT-INTO-SELECT statements") {
      val results = SQLLanguageParser.parse(
        """|INSERT INTO TABLE OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      val fields: List[Field] = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry)
      assert(results == Insert(Into(Table("OilGasSecurities")),
        Select(
          fields = fields,
          from = Table("Securities"),
          where = Field('Industry) === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support INSERT-INTO-VALUES statements") {
      val results = SQLLanguageParser.parse(
        """|INSERT INTO TABLE OilGasSecurities (Symbol, Name, Sector, Industry, LastSale)
           |VALUES
           |  ('AAPL', 'Apple, Inc.', 'Technology', 'Computers', 203.45),
           |  ('AMD', 'American Micro Devices, Inc.', 'Technology', 'Computers', 22.33)
           |""".stripMargin)
      val fields: List[Field] = List('Symbol, 'Name, 'Sector, 'Industry, 'LastSale)
      assert(results == Insert(Into(Table("OilGasSecurities")),
        Insert.Values(
          values = List(
            List("AAPL", "Apple, Inc.", "Technology", "Computers", 203.45),
            List("AMD", "American Micro Devices, Inc.", "Technology", "Computers", 22.33)
          )),
        fields = fields
      ))
    }

    it("should support INSERT-INTO-LOCATION-SELECT statements") {
      val results = SQLLanguageParser.parse(
        """|INSERT INTO LOCATION '/dir/subdir' (Symbol, Name, Sector, Industry, LastSale)
           |SELECT Symbol, Name, Sector, Industry, LastSale
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      val fields: List[Field] = List('Symbol, 'Name, 'Sector, 'Industry, 'LastSale)
      assert(results == Insert(Into(LocationRef("/dir/subdir")),
        Select(
          fields = fields,
          from = Table("Securities"),
          where = Field('Industry) === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support INSERT-OVERWRITE-SELECT statements") {
      val results = SQLLanguageParser.parse(
        """|INSERT OVERWRITE TABLE OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      val fields: List[Field] = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry)
      assert(results == Insert(Overwrite(Table("OilGasSecurities")),
        Select(
          fields = fields,
          from = Table("Securities"),
          where = Field('Industry) === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support INSERT-OVERWRITE-VALUES statements") {
      val results = SQLLanguageParser.parse(
        """|INSERT OVERWRITE TABLE OilGasSecurities (Symbol, Name, Sector, Industry, LastSale)
           |VALUES
           |  ('AAPL', 'Apple, Inc.', 'Technology', 'Computers', 203.45),
           |  ('AMD', 'American Micro Devices, Inc.', 'Technology', 'Computers', 22.33)
           |""".stripMargin)
      assert(results == Insert(Overwrite(Table("OilGasSecurities")),
        Insert.Values(values = List(
          List("AAPL", "Apple, Inc.", "Technology", "Computers", 203.45),
          List("AMD", "American Micro Devices, Inc.", "Technology", "Computers", 22.33)
        )),
        fields = List('Symbol, 'Name, 'Sector, 'Industry, 'LastSale)
      ))
    }

    it("should support INSERT-OVERWRITE-LOCATION-SELECT statements") {
      val results = SQLLanguageParser.parse(
        """|INSERT OVERWRITE LOCATION '/dir/subdir' (Symbol, Name, Sector, Industry, LastSale)
           |SELECT Symbol, Name, Sector, Industry, LastSale
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      val fields: List[Field] = List('Symbol, 'Name, 'Sector, 'Industry, 'LastSale)
      assert(results == Insert(Overwrite(LocationRef("/dir/subdir")),
        Select(
          fields = fields,
          from = Table("Securities"),
          where = Field('Industry) === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support simple SELECT statements without a FROM clause") {
      val results = SQLLanguageParser.parse("SELECT `$$DATA_SOURCE_ID` AS DATA_SOURCE_ID")
      assert(results == Select(fields = List(Field("$$DATA_SOURCE_ID").as("DATA_SOURCE_ID"))))
    }

    it("should support SELECT DISTINCT statements") {
      val results = SQLLanguageParser.parse(
        "SELECT DISTINCT Ticker_Symbol FROM Securities WHERE Industry = 'Oil/Gas Transmission'")
      assert(results ==
        Select(
          fields = List(Distinct('Ticker_Symbol)),
          from = Table("Securities"),
          where = Field('Industry) === "Oil/Gas Transmission")
      )
    }

    it("should support SELECT ... INTO statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |INTO TABLE OilGasSecurities
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      val fields: List[Field] = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry)
      assert(results == Insert(Into(Table("OilGasSecurities")),
        Select(
          fields = fields,
          from = Table("Securities"),
          where = Field('Industry) === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support SELECT ... EXISTS(...) statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT * FROM Departments WHERE EXISTS(SELECT employee_id FROM Employees WHERE role = 'MANAGER')
           |""".stripMargin)
      assert(results ==
        Select(Seq('*),
          from = Table("Departments"),
          where = Exists(Select(fields = Seq('employee_id), from = Table("Employees"), where = Field('role) === "MANAGER"))
        ))
    }

    it("should support SELECT ... FILESYSTEM(...) statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT * FROM (FILESYSTEM('models/'))
           |WHERE name like '%.csv'
           |ORDER BY name DESC
           |""".stripMargin)
      assert(results == Select(Seq('*), from = FileSystem("models/"), where = LIKE('name, "%.csv"), orderBy = Seq('name desc)))
    }

    it("should support SELECT ... GROUP BY statements") {
      import NativeFunctions._
      val results = SQLLanguageParser.parse(
        """|SELECT Sector, Industry, AVG(LastSale) AS LastSale, COUNT(*) AS total, COUNT(DISTINCT(*)) AS distinctTotal
           |FROM Customers
           |GROUP BY Sector, Industry
           |""".stripMargin)
      assert(results == Select(
        fields = List('Sector, 'Industry, avg('LastSale).as('LastSale),
          count('*).as('total), count(Distinct('*)).as('distinctTotal)),
        from = Table("Customers"),
        groupBy = List('Sector, 'Industry)
      ))
    }

    it("should support SELECT ... LIMIT n statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT Symbol, Name, Sector, Industry
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |LIMIT 100
           |""".stripMargin)
      assert(results == Select(
        fields = List('Symbol, 'Name, 'Sector, 'Industry),
        from = Table("Customers"),
        where = Field('Industry) === "Oil/Gas Transmission",
        limit = 100
      ))
    }

    it("should support SELECT TOP n ... statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT TOP 20 Symbol, Name, Sector, Industry
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == Select(
        fields = List('Symbol, 'Name, 'Sector, 'Industry),
        from = Table("Customers"),
        where = Field('Industry) === "Oil/Gas Transmission",
        limit = 20
      ))
    }

    it("should support SELECT ... ORDER BY statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |ORDER BY Symbol DESC, Name ASC
           |""".stripMargin)
      assert(results == Select(
        fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
        from = Table("Customers"),
        where = Field('Industry) === "Oil/Gas Transmission",
        orderBy = List('Symbol desc, 'Name asc)
      ))
    }

    it("should support SELECT ... OVER statements") {
      import NativeFunctions._
      val results = SQLLanguageParser.parse(
        """|SELECT PAT_ID, DEPT_ID, INS_AMT,
           |MIN(INS_AMT) OVER (PARTITION BY DEPT_ID ORDER BY DEPT_ID ASC) AS MIN_INS_AMT,
           |MAX(INS_AMT) OVER (PARTITION BY DEPT_ID ORDER BY DEPT_ID ASC) AS MAX_INS_AMT
           |FROM PATIENT
           |""".stripMargin
      )
      assert(results == Select(
        fields = List('PAT_ID, 'DEPT_ID, 'INS_AMT,
          min('INS_AMT).over(partitionBy = Seq('DEPT_ID), orderBy = Seq('DEPT_ID asc)).as("MIN_INS_AMT"),
          max('INS_AMT).over(partitionBy = Seq('DEPT_ID), orderBy = Seq('DEPT_ID asc)).as("MAX_INS_AMT")
        ),
        from = Table("PATIENT")
      ))
    }

    it("should support SELECT ... OVER w/RANGE statements") {
      import IntervalTypes._
      import NativeFunctions._
      import Over.DataAccessTypes._
      import Over._
      import com.qwery.util.OptionHelper.Implicits.Risky._

      val results = SQLLanguageParser.parse(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote, LastSale,
           |       MEAN(LastSale) OVER (
           |          PARTITION BY Symbol
           |          ORDER BY TradeDate ASC
           |          RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
           |       ) AS LastSaleMean
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == Select(
        fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote, 'LastSale,
          mean('LastSale)
            .over(partitionBy = Seq('Symbol),
              orderBy = Seq('TradeDate.asc),
              modifier = WindowBetween(RANGE, Preceding(Interval(7, DAY)), CurrentRow))
            .as("LastSaleMean")),
        from = Table("Customers"),
        where = Field('Industry) === "Oil/Gas Transmission"
      ))
    }

    it("should support SELECT ... OVER w/ROWS statements") {
      import IntervalTypes._
      import NativeFunctions._
      import Over.DataAccessTypes._
      import Over._
      import com.qwery.util.OptionHelper.Implicits.Risky._

      val results = SQLLanguageParser.parse(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote, LastSale,
           |       MEAN(LastSale) OVER (
           |          PARTITION BY Symbol
           |          ORDER BY TradeDate ASC
           |          ROWS BETWEEN INTERVAL 7 DAYS FOLLOWING AND CURRENT ROW
           |       ) AS LastSaleMean
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == Select(
        fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote, 'LastSale,
          mean('LastSale)
            .over(partitionBy = Seq('Symbol),
              orderBy = Seq('TradeDate.asc),
              modifier = WindowBetween(ROWS, Following(Interval(7, DAY)), CurrentRow))
            .as("LastSaleMean")),
        from = Table("Customers"),
        where = Field('Industry) === "Oil/Gas Transmission"
      ))
    }

    it("should support SELECT ... OVER w/ROWS UNBOUNDED statements") {
      import NativeFunctions._
      import Over.DataAccessTypes._
      import Over._
      import com.qwery.util.OptionHelper.Implicits.Risky._

      val results = SQLLanguageParser.parse(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote, LastSale,
           |       MEAN(LastSale) OVER (
           |          PARTITION BY Symbol
           |          ORDER BY TradeDate ASC
           |          ROWS UNBOUNDED FOLLOWING
           |       ) AS LastSaleMean
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == Select(
        fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote, 'LastSale,
          mean('LastSale)
            .over(partitionBy = Seq('Symbol),
              orderBy = Seq('TradeDate.asc),
              modifier = Option(Following(Unbounded(ROWS))))
            .as("LastSaleMean")),
        from = Table("Customers"),
        where = Field('Industry) === "Oil/Gas Transmission"
      ))
    }

    it("should support SELECT ... EXCEPT statements") {
      val results = SQLLanguageParser.parse(
        s"""|SELECT Symbol, Name, Sector, Industry, SummaryQuote
            |FROM Customers
            |WHERE Industry = 'Oil/Gas Transmission'
            |   EXCEPT
            |SELECT Symbol, Name, Sector, Industry, SummaryQuote
            |FROM Customers
            |WHERE Industry = 'Computer Manufacturing'
            |""".stripMargin)
      assert(results == Except(
        query0 = Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = Field('Industry) === "Oil/Gas Transmission"),
        query1 = Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = Field('Industry) === "Computer Manufacturing")
      ))
    }

    it("should support SELECT ... INTERSECT statements") {
      val results = SQLLanguageParser.parse(
        s"""|SELECT Symbol, Name, Sector, Industry, SummaryQuote
            |FROM Customers
            |WHERE Industry = 'Oil/Gas Transmission'
            |   INTERSECT
            |SELECT Symbol, Name, Sector, Industry, SummaryQuote
            |FROM Customers
            |WHERE Industry = 'Computer Manufacturing'
            |""".stripMargin)
      assert(results == Intersect(
        query0 = Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = Field('Industry) === "Oil/Gas Transmission"),
        query1 = Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = Field('Industry) === "Computer Manufacturing")
      ))
    }

    it("should support SELECT ... UNION statements") {
      Seq("ALL", "DISTINCT", "") foreach { modifier =>
        val results = SQLLanguageParser.parse(
          s"""|SELECT Symbol, Name, Sector, Industry, SummaryQuote
              |FROM Customers
              |WHERE Industry = 'Oil/Gas Transmission'
              |   UNION $modifier
              |SELECT Symbol, Name, Sector, Industry, SummaryQuote
              |FROM Customers
              |WHERE Industry = 'Computer Manufacturing'
              |""".stripMargin)
        assert(results == Union(
          query0 = Select(
            fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
            from = Table("Customers"),
            where = Field('Industry) === "Oil/Gas Transmission"),
          query1 = Select(
            fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
            from = Table("Customers"),
            where = Field('Industry) === "Computer Manufacturing"),
          isDistinct = modifier == "DISTINCT"
        ))
      }
    }

    it("should support SELECT ... WHERE BETWEEN statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE IPOYear BETWEEN '2000' AND '2019'
           |""".stripMargin)
      assert(results == Select(
        fields = Seq('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
        from = Table("Customers"),
        where = Between('IPOYear, "2000", "2019")
      ))
    }

    it("should support SELECT ... WHERE IN (...) statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE IPOYear IN ('2000', '2001', '2003', '2008', '2019')
           |""".stripMargin)
      assert(results == Select(
        fields = Seq('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
        from = Table("Customers"),
        where = IN('IPOYear)("2000", "2001", "2003", "2008", "2019")
      ))
    }

    it("should support SELECT ... WHERE IN (SELECT ...) statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers AS C
           |WHERE IPOYear IN (SELECT `Year` FROM EligibleYears)
           |""".stripMargin)
      assert(results == Select(
        fields = Seq('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
        from = Table("Customers"),
        where = IN('IPOYear, Select(fields = Seq('Year), from = Table("EligibleYears")))
      ))
    }

    it("should support SELECT w/CROSS JOIN statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |CROSS JOIN CustomerAddresses as CA ON CA.customerId = C.customerId
           |CROSS JOIN Addresses as A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(Field.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), Field("CA.customerId") === Field("C.customerId"), JoinTypes.CROSS),
          Join(Table("Addresses").as("A"), Field("A.addressId") === Field("CA.addressId"), JoinTypes.CROSS)
        ),
        where = Field("C.firstName") === "Lawrence" && Field("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/INNER JOIN statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers as C
           |INNER JOIN CustomerAddresses as CA ON CA.customerId = C.customerId
           |INNER JOIN Addresses as A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(Field.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), Field("CA.customerId") === Field("C.customerId"), JoinTypes.INNER),
          Join(Table("Addresses").as("A"), Field("A.addressId") === Field("CA.addressId"), JoinTypes.INNER)
        ),
        where = Field("C.firstName") === "Lawrence" && Field("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/FULL OUTER JOIN statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers as C
           |FULL OUTER JOIN CustomerAddresses as CA ON CA.customerId = C.customerId
           |FULL OUTER JOIN Addresses as A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(Field.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), Field("CA.customerId") === Field("C.customerId"), JoinTypes.FULL_OUTER),
          Join(Table("Addresses").as("A"), Field("A.addressId") === Field("CA.addressId"), JoinTypes.FULL_OUTER)
        ),
        where = Field("C.firstName") === "Lawrence" && Field("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/LEFT OUTER JOIN statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers as C
           |LEFT OUTER JOIN CustomerAddresses as CA ON CA.customerId = C.customerId
           |LEFT OUTER JOIN Addresses as A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(Field.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), Field("CA.customerId") === Field("C.customerId"), JoinTypes.LEFT_OUTER),
          Join(Table("Addresses").as("A"), Field("A.addressId") === Field("CA.addressId"), JoinTypes.LEFT_OUTER)
        ),
        where = Field("C.firstName") === "Lawrence" && Field("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/RIGHT OUTER JOIN statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers as C
           |RIGHT OUTER JOIN CustomerAddresses as CA ON CA.customerId = C.customerId
           |RIGHT OUTER JOIN Addresses as A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(Field.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), Field("CA.customerId") === Field("C.customerId"), JoinTypes.RIGHT_OUTER),
          Join(Table("Addresses").as("A"), Field("A.addressId") === Field("CA.addressId"), JoinTypes.RIGHT_OUTER)
        ),
        where = Field("C.firstName") === "Lawrence" && Field("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/JOIN ... USING statements") {
      val results = SQLLanguageParser.parse(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers as C
           |JOIN CustomerAddresses as CA USING customerId
           |JOIN Addresses as A USING addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(Field.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), columns = Seq("customerId"), JoinTypes.INNER),
          Join(Table("Addresses").as("A"), columns = Seq("addressId"), JoinTypes.INNER)
        ),
        where = Field("C.firstName") === "Lawrence" && Field("C.lastName") === "Daniels"
      ))
    }

    it("should support SET local variable statements") {
      val results = SQLLanguageParser.parse("SET @customers = @customers + 1")
      assert(results == SetLocalVariable(name = "customers", @@("customers") + 1))
    }

    it("should support SET row variable statements") {
      val results = SQLLanguageParser.parse(
        """|SET #securities = (
           |  SELECT Symbol, Name, Sector, Industry, `Summary Quote`
           |  FROM Securities
           |  WHERE Industry = 'Oil/Gas Transmission'
           |  ORDER BY Symbol ASC
           |)
           |""".stripMargin)
      assert(results == SetRowVariable(name = "securities",
        Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, Field("Summary Quote")),
          from = Table("Securities"),
          where = Field('Industry) === "Oil/Gas Transmission",
          orderBy = List('Symbol asc)
        )))
    }

    it("should support SHOW statements") {
      val results = SQLLanguageParser.parse("SHOW #theResults LIMIT 5")
      assert(results == Show(rows = @#("theResults"), limit = 5))
    }

    it("should support UPDATE statements") {
      val results = SQLLanguageParser.parse(
        """|UPDATE Companies
           |SET Symbol = 'AAPL', Name = 'Apple, Inc.', Sector = 'Technology', Industry = 'Computers', LastSale = 203.45
           |WHERE Symbol = 'AAPL'
           |""".stripMargin)
      assert(results == Update(
        table = Table("Companies"),
        assignments = Seq(
          "Symbol" -> "AAPL", "Name" -> "Apple, Inc.",
          "Sector" -> "Technology", "Industry" -> "Computers", "LastSale" -> 203.45
        ),
        where = Field('Symbol) === "AAPL"
      ))
    }

    it("should support WHILE statements") {
      val results = SQLLanguageParser.parse(
        """|WHILE @cnt < 10
           |BEGIN
           |   PRINT 'Hello World';
           |   SET @cnt = @cnt + 1;
           |END;
           |""".stripMargin)
      val cnt = @@("cnt")
      assert(results == While(
        condition = cnt < 10,
        invokable = SQL(
          Print("Hello World"),
          SetLocalVariable(cnt.name, cnt + 1)
        )
      ))
    }

  }

}
