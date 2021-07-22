package com.qwery.language

import com.qwery.models.AlterTable._
import com.qwery.models.Console.{Print, Println}
import com.qwery.models.Insert.{Into, Overwrite}
import com.qwery.models._
import com.qwery.models.expressions._
import org.scalatest.funspec.AnyFunSpec

import scala.language.postfixOps

/**
  * SQL Language Parser Test
  * @author lawrence.daniels@gmail.com
  */
class SQLCompilerTest extends AnyFunSpec {

  describe(classOf[SQLCompiler].getSimpleName) {
    import com.qwery.models.expressions.implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("should support ALTER TABLE .. ADD") {
      val results = SQLCompiler.compile("ALTER TABLE stocks ADD comments TEXT")
      assert(results == AlterTable(EntityRef.parse("stocks"), AddColumn(Column("comments TEXT"))))
    }

    it("should support ALTER TABLE .. ADD COLUMN .. DEFAULT") {
      val results = SQLCompiler.compile("ALTER TABLE stocks ADD COLUMN comments TEXT DEFAULT 'N/A'")
      assert(results == AlterTable(EntityRef.parse("stocks"), AddColumn(Column("comments TEXT").copy(defaultValue = Some("N/A")))))
    }

    it("should support ALTER TABLE .. APPEND") {
      val results = SQLCompiler.compile("ALTER TABLE stocks APPEND comments TEXT DEFAULT 'N/A'")
      assert(results == AlterTable(EntityRef.parse("stocks"), AppendColumn(Column("comments TEXT").copy(defaultValue = Some("N/A")))))
    }

    it("should support ALTER TABLE .. APPEND COLUMN") {
      val results = SQLCompiler.compile("ALTER TABLE stocks APPEND COLUMN comments TEXT")
      assert(results == AlterTable(EntityRef.parse("stocks"), AppendColumn(Column("comments TEXT"))))
    }

    it("should support ALTER TABLE .. DROP") {
      val results = SQLCompiler.compile("ALTER TABLE stocks DROP comments")
      assert(results == AlterTable(EntityRef.parse("stocks"), DropColumn("comments")))
    }

    it("should support ALTER TABLE .. DROP COLUMN") {
      val results = SQLCompiler.compile("ALTER TABLE stocks DROP COLUMN comments")
      assert(results == AlterTable(EntityRef.parse("stocks"), DropColumn("comments")))
    }

    it("should support ALTER TABLE .. PREPEND") {
      val results = SQLCompiler.compile("ALTER TABLE stocks PREPEND comments TEXT")
      assert(results == AlterTable(EntityRef.parse("stocks"), PrependColumn(Column("comments TEXT"))))
    }

    it("should support ALTER TABLE .. PREPEND COLUMN") {
       val results = SQLCompiler.compile("ALTER TABLE stocks PREPEND COLUMN comments TEXT")
      assert(results == AlterTable(EntityRef.parse("stocks"), PrependColumn(Column("comments TEXT"))))
    }

    it("should support ALTER TABLE .. RENAME") {
      val results = SQLCompiler.compile("ALTER TABLE stocks RENAME comments AS remarks")
      assert(results == AlterTable(EntityRef.parse("stocks"), RenameColumn(oldName = "comments", newName = "remarks")))
    }

    it("should support ALTER TABLE .. RENAME COLUMN") {
      val results = SQLCompiler.compile("ALTER TABLE stocks RENAME COLUMN comments AS remarks")
      assert(results == AlterTable(EntityRef.parse("stocks"), RenameColumn(oldName = "comments", newName = "remarks")))
    }

    it("should support ALTER TABLE .. ADD/DROP") {
      val results = SQLCompiler.compile("ALTER TABLE stocks ADD comments TEXT DROP remarks")
      assert(results == AlterTable(EntityRef.parse("stocks"), Seq(AddColumn(Column("comments TEXT")), DropColumn("remarks"))))
    }

    it("should support ALTER TABLE .. ADD COLUMN/DROP COLUMN") {
      val results = SQLCompiler.compile("ALTER TABLE stocks ADD COLUMN comments TEXT DROP COLUMN remarks")
      assert(results == AlterTable(EntityRef.parse("stocks"), Seq(AddColumn(Column("comments TEXT")), DropColumn("remarks"))))
    }

    it("should support BEGIN .. END") {
      val results = SQLCompiler.compile(
        """|BEGIN
           |  PRINT 'Hello ';
           |  PRINTLN 'World';
           |END
           |""".stripMargin)
      assert(results == SQL(Console.Print("Hello "), Console.Println("World")))
    }

    it("should support { .. }") {
      val results = SQLCompiler.compile(
        """|{
           |  INFO 'Hello World'
           |}
           |""".stripMargin)
      assert(results == Console.Info("Hello World"))
    }

    it("should support CALL") {
      val results = SQLCompiler.compile("CALL computeArea(length, width)")
      assert(results == ProcedureCall(name = "computeArea", args = List('length, 'width)))
    }

    it("should support CREATE EXTERNAL TABLE") {
      val results = SQLCompiler.compile(
        """|CREATE EXTERNAL TABLE Customers (customer_uid UUID, name STRING, address STRING, ingestion_date LONG)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |STORED AS INPUTFORMAT 'JSON' OUTPUTFORMAT 'JSON'
           |LOCATION './dataSets/customers/json/'
           |""".stripMargin)
      assert(results == Create(ExternalTable(
        ref = EntityRef.parse("Customers"),
        columns = List("customer_uid UUID", "name STRING", "address STRING", "ingestion_date LONG").map(Column.apply),
        partitionBy = List("year STRING", "month STRING", "day STRING").map(Column.apply),
        inputFormat = "json", outputFormat = "json",
        location = Some("./dataSets/customers/json/")
      )))
    }

    it("should support CREATE EXTERNAL TABLE w/COMMENT") {
      val results = SQLCompiler.compile(
        """|CREATE EXTERNAL TABLE Customers (
           |    customer_uid UUID COMMENT 'Unique Customer ID',
           |    name STRING COMMENT '',
           |    address STRING COMMENT '',
           |    ingestion_date LONG COMMENT ''
           |)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |ROW FORMAT DELIMITED
           |FIELDS TERMINATED BY ','
           |STORED AS INPUTFORMAT 'CSV'
           |WITH COMMENT 'Customer information'
           |WITH HEADERS ON
           |WITH NULL VALUES AS 'n/a'
           |LOCATION './dataSets/customers/csv/'
           |""".stripMargin)
      assert(results == Create(ExternalTable(ref = EntityRef.parse("Customers"), description = Some("Customer information"),
        columns = List(
          Column("customer_uid UUID").copy(comment = Some("Unique Customer ID")),
          Column("name STRING"), Column("address STRING"), Column("ingestion_date LONG")
        ),
        partitionBy = List("year STRING", "month STRING", "day STRING").map(Column.apply),
        fieldTerminator = ",",
        headersIncluded = true,
        nullValue = Some("n/a"),
        inputFormat = "csv",
        location = Some("./dataSets/customers/csv/")
      )))
    }

    it("should support CREATE EXTERNAL TABLE .. WITH") {
      val results = SQLCompiler.compile(
        """|CREATE EXTERNAL TABLE Customers (customer_uid UUID, name STRING, address STRING, ingestion_date LONG)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |ROW FORMAT DELIMITED
           |FIELDS TERMINATED BY ','
           |STORED AS INPUTFORMAT 'CSV'
           |WITH HEADERS ON
           |WITH NULL VALUES AS 'n/a'
           |LOCATION './dataSets/customers/csv/'
           |""".stripMargin)
      assert(results == Create(ExternalTable(
        ref = EntityRef.parse("Customers"),
        columns = List("customer_uid UUID", "name STRING", "address STRING", "ingestion_date LONG").map(Column.apply),
        partitionBy = List("year STRING", "month STRING", "day STRING").map(Column.apply),
        fieldTerminator = ",",
        headersIncluded = true,
        nullValue = Some("n/a"),
        inputFormat = "csv",
        location = Some("./dataSets/customers/csv/")
      )))
    }

    it("should support CREATE EXTERNAL TABLE .. PARTITIONED BY") {
      val results = SQLCompiler.compile(
        """|CREATE EXTERNAL TABLE revenue_per_page (
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
           |  'csv'
           |LOCATION
           |  's3://kbb-etl-mart-dev/kbb_rev_per_page/kbb_rev_per_page'
           |TBLPROPERTIES (
           |  'transient_lastDdlTime'='1555400098')
           |""".stripMargin)
      assert(results == Create(ExternalTable(
        ref = EntityRef.parse("revenue_per_page"),
        columns = List(
          Column("rank string"),
          Column("section string"),
          Column("super_section string"),
          Column("page_name string"),
          Column("rev string"),
          Column("last_processed_ts_est string")
        ).map(_.copy(comment = Some("from deserializer"))),
        partitionBy = List(Column("hit_dt_est string"), Column("site_experience_desc string")),
        inputFormat = "csv",
        location = Some("s3://kbb-etl-mart-dev/kbb_rev_per_page/kbb_rev_per_page"),
        serdeProperties = Map("quoteChar" -> "\\\"", "separatorChar" -> ","),
        tableProperties = Map("transient_lastDdlTime" -> "1555400098")
      )))
    }

    it("should support CREATE FUNCTION") {
      val results = SQLCompiler.compile("CREATE FUNCTION myFunc AS 'com.qwery.udf.MyFunc'")
      assert(results == Create(UserDefinedFunction(
        ref = EntityRef.parse("myFunc"),
        `class` = "com.qwery.udf.MyFunc",
        ifNotExists = false,
        description = None,
        jarLocation = None)))
    }

    it("should support CREATE FUNCTION .. USING JAR") {
      val results = SQLCompiler.compile(
        """|CREATE FUNCTION myFunc AS 'com.qwery.udf.MyFunc'
           |USING JAR '/home/ldaniels/shocktrade/jars/shocktrade-0.8.jar'
           |""".stripMargin)
      assert(results == Create(UserDefinedFunction(
        ref = EntityRef.parse("myFunc"),
        `class` = "com.qwery.udf.MyFunc",
        jarLocation = "/home/ldaniels/shocktrade/jars/shocktrade-0.8.jar",
        ifNotExists = false,
        description = None
      )))
    }

    it("should support CREATE INDEX") {
      val results = SQLCompiler.compile(
        """|CREATE INDEX stocks_symbol ON stocks (symbol)
           |""".stripMargin)
      assert(results == Create(TableIndex(
        ref = EntityRef.parse("stocks_symbol"),
        columns = List("symbol"),
        table = EntityRef.parse("stocks"),
        ifNotExists = false
      )))
    }

    it("should support CREATE PROCEDURE") {
      val results = SQLCompiler.compile(
        """|CREATE PROCEDURE testInserts(industry STRING) AS
           |  RETURN (
           |    SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |    FROM Customers
           |    WHERE Industry = $industry
           |  )
           |""".stripMargin)
      assert(results == Create(Procedure(ref = EntityRef.parse("testInserts"),
        params = List("industry STRING").map(Column.apply),
        code = Return(Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = FieldRef('Industry) === $$(name = "industry")
        ))
      )))
    }

    it("should support CREATE TABLE w/ENUM") {
      val results = SQLCompiler.compile(
        s"""|CREATE TABLE Stocks (
            |  symbol STRING,
            |  exchange STRING AS ENUM ('AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHEROTC'),
            |  lastSale DOUBLE,
            |  lastSaleTime DATE
            |)
            |""".stripMargin
      )
      assert(results == Create(Table(
        ref = EntityRef.parse("Stocks"),
        columns = List(
          Column("symbol STRING"), Column("exchange STRING").copy(enumValues = Seq("AMEX", "NASDAQ", "NYSE", "OTCBB", "OTHEROTC")),
          Column("lastSale DOUBLE"), Column("lastSaleTime DATE"))
      )))
    }

    it("should support CREATE TABLE with inline VALUES") {
      val results = SQLCompiler.compile(
        """|CREATE TABLE SpecialSecurities (symbol STRING, lastSale DOUBLE)
           |FROM VALUES ('AAPL', 202.11), ('AMD', 23.50), ('GOOG', 765.33), ('AMZN', 699.01)
           |""".stripMargin)
      val ref = EntityRef.parse("SpecialSecurities")
      assert(results == SQL(
        Create(Table(ref, columns = List("symbol STRING", "lastSale DOUBLE").map(Column.apply))),
        Insert(Into(ref), source = Insert.Values(List(List("AAPL", 202.11), List("AMD", 23.50), List("GOOG", 765.33), List("AMZN", 699.01))))
      ))
    }

    it("should support CREATE TABLE with subquery") {
      val results = SQLCompiler.compile(
        """|CREATE TABLE SpecialSecurities (symbol STRING, lastSale DOUBLE)
           |FROM (
           |    SELECT symbol, lastSale
           |    FROM Securities
           |    WHERE useCode = 'SPECIAL'
           |)
           |""".stripMargin)
      val ref = EntityRef.parse("SpecialSecurities")
      assert(results == SQL(
        Create(Table(ref, columns = List("symbol STRING", "lastSale DOUBLE").map(Column.apply))),
        Insert(Into(ref), source = Select(fields = Seq('symbol, 'lastSale), from = Some(Table("Securities")), where = FieldRef('useCode) === "SPECIAL"))
      ))
    }

    it("should support CREATE TYPE .. AS ENUM") {
      val results = SQLCompiler.compile(
        """|CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')
           |""".stripMargin
      )
      assert(results == Create(TypeAsEnum(ref = EntityRef.parse("mood"), values = Seq("sad", "ok", "happy"))))
    }

    it("should support CREATE VIEW") {
      val results = SQLCompiler.compile(
        """|CREATE VIEW IF NOT EXISTS OilAndGas AS
           |SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == Create(View(ref = EntityRef.parse("OilAndGas"), ifNotExists = true,
        query = Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = FieldRef('Industry) === "Oil/Gas Transmission"
        ))))
    }

    it("should support CREATE VIEW .. WITH COMMENT") {
      val results = SQLCompiler.compile(
        """|CREATE VIEW IF NOT EXISTS OilAndGas
           |WITH COMMENT 'AMEX Stock symbols sorted by last sale'
           |AS
           |SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == Create(View(ref = EntityRef.parse("OilAndGas"), description = Some("AMEX Stock symbols sorted by last sale"), ifNotExists = true,
        query = Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = FieldRef('Industry) === "Oil/Gas Transmission"
        ))))
    }

    it("should support DECLARE") {
      val results = SQLCompiler.compile("DECLARE $customerId INTEGER")
      assert(results == Declare(variable = $$("customerId"), `type` = "INTEGER"))
    }

    it("should support DELETE") {
      val results = SQLCompiler.compile("DELETE FROM todo_list where item_id = 1238 LIMIT 25")
      assert(results == Delete(table = Table("todo_list"), where = FieldRef('item_id) === 1238, limit = Some(25)))
    }

    it("should support DEBUG, ERROR, INFO, LOG, PRINT and WARN") {
      case class Expected(command: String, opCode: String => Console, message: String)
      val tests = Seq(
        Expected("DEBUG", Console.Debug.apply, "This is a debug message"),
        Expected("ERROR", Console.Error.apply, "This is an error message"),
        Expected("INFO", Console.Info.apply, "This is an informational message"),
        Expected("PRINT", Console.Print.apply, "This message will be printed to STDOUT"),
        Expected("WARN", Console.Warn.apply, "This is a warning message"))
      tests foreach { case Expected(command, opCode, message) =>
        val results = SQLCompiler.compile(s"$command '$message'")
        assert(results == opCode(message))
      }
    }

    it("should support FOR .. IN") {
      val results = SQLCompiler.compile(
        """|FOR $item IN (SELECT symbol, lastSale FROM Securities WHERE naics = '12345') {
           |  PRINTLN '{{item.symbol}} is {{item.lastSale}}/share';
           |}
           |""".stripMargin)
      assert(results == ForEach(
        variable = $$("item"),
        rows = Select(fields = Seq('symbol, 'lastSale), from = Table("Securities"), where = FieldRef('naics) === "12345"),
        invokable = SQL(
          Println("{{item.symbol}} is {{item.lastSale}}/share")
        ),
        isReverse = false
      ))
    }

    it("should support FOR .. LOOP") {
      val results = SQLCompiler.compile(
        """|FOR $item IN REVERSE (SELECT symbol, lastSale FROM Securities WHERE naics = '12345')
           |LOOP
           |  PRINTLN '{{item.symbol}} is {{item.lastSale}}/share';
           |END LOOP;
           |""".stripMargin)
      assert(results == ForEach(
        variable = $$("item"),
        rows = Select(fields = Seq('symbol, 'lastSale), from = Table("Securities"), where = FieldRef('naics) === "12345"),
        invokable = SQL(
          Println("{{item.symbol}} is {{item.lastSale}}/share")
        ),
        isReverse = true
      ))
    }

    it("should support INCLUDE") {
      val results = SQLCompiler.compile("INCLUDE 'models/securities.sql'")
      assert(results == Include("models/securities.sql"))
    }

    it("should support INSERT without explicitly defined fields") {
      val results = SQLCompiler.compile(
        "INSERT INTO Students VALUES ('Fred Flintstone', 35, 1.28), ('Barney Rubble', 32, 2.32)")
      assert(results == Insert(Into(Table("Students")), Insert.Values(values = List(
        List("Fred Flintstone", 35.0, 1.28),
        List("Barney Rubble", 32.0, 2.32)
      ))))
    }

    it("should support INSERT-INTO-SELECT") {
      val results = SQLCompiler.compile(
        """|INSERT INTO TABLE OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      val fields: List[FieldRef] = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry)
      assert(results == Insert(Into(Table("OilGasSecurities")),
        Select(
          fields = fields,
          from = Table("Securities"),
          where = FieldRef('Industry) === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support INSERT-INTO-VALUES") {
      val results = SQLCompiler.compile(
        """|INSERT INTO TABLE OilGasSecurities (Symbol, Name, Sector, Industry, LastSale)
           |VALUES
           |  ('AAPL', 'Apple, Inc.', 'Technology', 'Computers', 203.45),
           |  ('AMD', 'American Micro Devices, Inc.', 'Technology', 'Computers', 22.33)
           |""".stripMargin)
      val fields: List[FieldRef] = List('Symbol, 'Name, 'Sector, 'Industry, 'LastSale)
      assert(results == Insert(Into(Table("OilGasSecurities")),
        Insert.Values(
          values = List(
            List("AAPL", "Apple, Inc.", "Technology", "Computers", 203.45),
            List("AMD", "American Micro Devices, Inc.", "Technology", "Computers", 22.33)
          )),
        fields = fields
      ))
    }

    it("should support INSERT-OVERWRITE-SELECT") {
      val results = SQLCompiler.compile(
        """|INSERT OVERWRITE TABLE OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      val fields: List[FieldRef] = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry)
      assert(results == Insert(Overwrite(Table("OilGasSecurities")),
        Select(
          fields = fields,
          from = Table("Securities"),
          where = FieldRef('Industry) === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support INSERT-OVERWRITE-VALUES") {
      val results = SQLCompiler.compile(
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

    it("should support SELECT without a FROM clause") {
      val results = SQLCompiler.compile("SELECT `$$DATA_SOURCE_ID` AS DATA_SOURCE_ID")
      assert(results == Select(fields = List(FieldRef("$$DATA_SOURCE_ID").as("DATA_SOURCE_ID"))))
    }

    it("should support SELECT function call") {
      val results = SQLCompiler.compile(
        "SELECT symbol, customFx(lastSale) FROM Securities WHERE naics = '12345'")
      assert(results == Select(
        fields = Seq('symbol, FunctionCall("customFx")('lastSale)),
        from = Table("Securities"),
        where = FieldRef('naics) === "12345"
      ))
    }

    it("should support SELECT DISTINCT") {
      val results = SQLCompiler.compile(
        "SELECT DISTINCT Ticker_Symbol FROM Securities WHERE Industry = 'Oil/Gas Transmission'")
      assert(results ==
        Select(
          fields = List(Distinct('Ticker_Symbol)),
          from = Table("Securities"),
          where = FieldRef('Industry) === "Oil/Gas Transmission")
      )
    }

    it("should support SELECT .. INTO") {
      val results = SQLCompiler.compile(
        """|SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |INTO TABLE OilGasSecurities
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
      val fields: List[FieldRef] = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry)
      assert(results == Insert(Into(Table("OilGasSecurities")),
        Select(
          fields = fields,
          from = Table("Securities"),
          where = FieldRef('Industry) === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support SELECT .. EXISTS(..)") {
      val results = SQLCompiler.compile(
        """|SELECT * FROM Departments WHERE EXISTS(SELECT employee_id FROM Employees WHERE role = 'MANAGER')
           |""".stripMargin)
      assert(results ==
        Select(Seq('*),
          from = Table("Departments"),
          where = Exists(Select(fields = Seq('employee_id), from = Table("Employees"), where = FieldRef('role) === "MANAGER"))
        ))
    }

    it("should support SELECT .. GROUP BY fields") {
      import NativeFunctions._
      val results = SQLCompiler.compile(
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

    it("should support SELECT .. GROUP BY indices") {
      import NativeFunctions._
      val results = SQLCompiler.compile(
        """|SELECT Sector, Industry, AVG(LastSale) AS LastSale, COUNT(*) AS total, COUNT(DISTINCT(*)) AS distinctTotal
           |FROM Customers
           |GROUP BY 1, 2
           |""".stripMargin)
      assert(results == Select(
        fields = List('Sector, 'Industry, avg('LastSale).as('LastSale),
          count('*).as('total), count(Distinct('*)).as('distinctTotal)),
        from = Table("Customers"),
        groupBy = List(FieldRef("1"), FieldRef("2"))
      ))
    }

    it("should support SELECT .. LIMIT n") {
      val results = SQLCompiler.compile(
        """|SELECT Symbol, Name, Sector, Industry
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |LIMIT 100
           |""".stripMargin)
      assert(results == Select(
        fields = List('Symbol, 'Name, 'Sector, 'Industry),
        from = Table("Customers"),
        where = FieldRef('Industry) === "Oil/Gas Transmission",
        limit = 100
      ))
    }

    it("should support SELECT .. ORDER BY") {
      val results = SQLCompiler.compile(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |ORDER BY Symbol DESC, Name ASC
           |""".stripMargin)
      assert(results == Select(
        fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
        from = Table("Customers"),
        where = FieldRef('Industry) === "Oil/Gas Transmission",
        orderBy = List('Symbol desc, 'Name asc)
      ))
    }

    it("should support SELECT .. OVER") {
      import NativeFunctions._
      val results = SQLCompiler.compile(
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

    it("should support SELECT .. OVER w/RANGE") {
      import IntervalTypes._
      import NativeFunctions._
      import Over.DataAccessTypes._
      import Over._
      import com.qwery.util.OptionHelper.Implicits.Risky._

      val results = SQLCompiler.compile(
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
        where = FieldRef('Industry) === "Oil/Gas Transmission"
      ))
    }

    it("should support SELECT .. OVER w/ROWS") {
      import IntervalTypes._
      import NativeFunctions._
      import Over.DataAccessTypes._
      import Over._
      import com.qwery.util.OptionHelper.Implicits.Risky._

      val results = SQLCompiler.compile(
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
        where = FieldRef('Industry) === "Oil/Gas Transmission"
      ))
    }

    it("should support SELECT .. OVER w/ROWS UNBOUNDED") {
      import NativeFunctions._
      import Over.DataAccessTypes._
      import Over._
      import com.qwery.util.OptionHelper.Implicits.Risky._

      val results = SQLCompiler.compile(
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
        where = FieldRef('Industry) === "Oil/Gas Transmission"
      ))
    }

    it("should support SELECT .. EXCEPT") {
      val results = SQLCompiler.compile(
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
          where = FieldRef('Industry) === "Oil/Gas Transmission"),
        query1 = Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = FieldRef('Industry) === "Computer Manufacturing")
      ))
    }

    it("should support SELECT .. INTERSECT") {
      val results = SQLCompiler.compile(
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
          where = FieldRef('Industry) === "Oil/Gas Transmission"),
        query1 = Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = FieldRef('Industry) === "Computer Manufacturing")
      ))
    }

    it("should support SELECT .. UNION") {
      Seq("ALL", "DISTINCT", "") foreach { modifier =>
        val results = SQLCompiler.compile(
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
            where = FieldRef('Industry) === "Oil/Gas Transmission"),
          query1 = Select(
            fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
            from = Table("Customers"),
            where = FieldRef('Industry) === "Computer Manufacturing"),
          isDistinct = modifier == "DISTINCT"
        ))
      }
    }

    it("should support SELECT .. WHERE BETWEEN") {
      val results = SQLCompiler.compile(
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

    it("should support SELECT .. WHERE IN (..)") {
      val results = SQLCompiler.compile(
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

    it("should support SELECT .. WHERE IN (SELECT ..)") {
      val results = SQLCompiler.compile(
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

    it("should support SELECT w/CROSS JOIN") {
      val results = SQLCompiler.compile(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |CROSS JOIN CustomerAddresses AS CA ON CA.customerId = C.customerId
           |CROSS JOIN Addresses AS A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(FieldRef.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), FieldRef("CA.customerId") === FieldRef("C.customerId"), JoinTypes.CROSS),
          Join(Table("Addresses").as("A"), FieldRef("A.addressId") === FieldRef("CA.addressId"), JoinTypes.CROSS)
        ),
        where = FieldRef("C.firstName") === "Lawrence" && FieldRef("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/INNER JOIN") {
      val results = SQLCompiler.compile(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |INNER JOIN CustomerAddresses AS CA ON CA.customerId = C.customerId
           |INNER JOIN Addresses AS A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(FieldRef.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), FieldRef("CA.customerId") === FieldRef("C.customerId"), JoinTypes.INNER),
          Join(Table("Addresses").as("A"), FieldRef("A.addressId") === FieldRef("CA.addressId"), JoinTypes.INNER)
        ),
        where = FieldRef("C.firstName") === "Lawrence" && FieldRef("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/FULL OUTER JOIN") {
      val results = SQLCompiler.compile(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |FULL OUTER JOIN CustomerAddresses AS CA ON CA.customerId = C.customerId
           |FULL OUTER JOIN Addresses AS A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(FieldRef.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), FieldRef("CA.customerId") === FieldRef("C.customerId"), JoinTypes.FULL_OUTER),
          Join(Table("Addresses").as("A"), FieldRef("A.addressId") === FieldRef("CA.addressId"), JoinTypes.FULL_OUTER)
        ),
        where = FieldRef("C.firstName") === "Lawrence" && FieldRef("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/LEFT OUTER JOIN") {
      val results = SQLCompiler.compile(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |LEFT OUTER JOIN CustomerAddresses AS CA ON CA.customerId = C.customerId
           |LEFT OUTER JOIN Addresses AS A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(FieldRef.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), FieldRef("CA.customerId") === FieldRef("C.customerId"), JoinTypes.LEFT_OUTER),
          Join(Table("Addresses").as("A"), FieldRef("A.addressId") === FieldRef("CA.addressId"), JoinTypes.LEFT_OUTER)
        ),
        where = FieldRef("C.firstName") === "Lawrence" && FieldRef("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/RIGHT OUTER JOIN") {
      val results = SQLCompiler.compile(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |RIGHT OUTER JOIN CustomerAddresses AS CA ON CA.customerId = C.customerId
           |RIGHT OUTER JOIN Addresses AS A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(FieldRef.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), FieldRef("CA.customerId") === FieldRef("C.customerId"), JoinTypes.RIGHT_OUTER),
          Join(Table("Addresses").as("A"), FieldRef("A.addressId") === FieldRef("CA.addressId"), JoinTypes.RIGHT_OUTER)
        ),
        where = FieldRef("C.firstName") === "Lawrence" && FieldRef("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/JOIN .. USING") {
      val results = SQLCompiler.compile(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |JOIN CustomerAddresses AS CA USING customerId
           |JOIN Addresses AS A USING addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(FieldRef.apply),
        from = Table("Customers").as("C"),
        joins = List(
          Join(Table("CustomerAddresses").as("CA"), columns = Seq("customerId"), JoinTypes.INNER),
          Join(Table("Addresses").as("A"), columns = Seq("addressId"), JoinTypes.INNER)
        ),
        where = FieldRef("C.firstName") === "Lawrence" && FieldRef("C.lastName") === "Daniels"
      ))
    }

    it("should support SET variable") {
      val results = SQLCompiler.compile("SET $customers = $customers + 1")
      assert(results == SetLocalVariable(name = "customers", $$("customers") + 1))
    }

    it("should support SET row variable") {
      val results = SQLCompiler.compile(
        """|SET @securities = (
           |  SELECT Symbol, Name, Sector, Industry, `Summary Quote`
           |  FROM Securities
           |  WHERE Industry = 'Oil/Gas Transmission'
           |  ORDER BY Symbol ASC
           |)
           |""".stripMargin)
      assert(results == SetRowVariable(name = "securities",
        Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, FieldRef("Summary Quote")),
          from = Table("Securities"),
          where = FieldRef('Industry) === "Oil/Gas Transmission",
          orderBy = List('Symbol asc)
        )))
    }

    it("should support SHOW") {
      val results = SQLCompiler.compile("SHOW @theResults LIMIT 5")
      assert(results == Show(rows = @@("theResults"), limit = 5))
    }

    it("should support TRUNCATE") {
      val results = SQLCompiler.compile("TRUNCATE stocks")
      assert(results == Truncate(table = EntityRef.parse("stocks")))
    }

    it("should support UPDATE") {
      val results = SQLCompiler.compile(
        """|UPDATE Companies
           |SET Symbol = 'AAPL', Name = 'Apple, Inc.', Sector = 'Technology', Industry = 'Computers', LastSale = 203.45
           |WHERE Symbol = 'AAPL'
           |LIMIT 25
           |""".stripMargin)
      assert(results == Update(
        table = Table("Companies"),
        changes = Seq(
          "Symbol" -> "AAPL", "Name" -> "Apple, Inc.",
          "Sector" -> "Technology", "Industry" -> "Computers", "LastSale" -> 203.45
        ),
        where = FieldRef('Symbol) === "AAPL",
        limit = Some(25)
      ))
    }

    it("should support DO ... WHILE") {
      val results = SQLCompiler.compile(
        """|DO
           |BEGIN
           |   PRINTLN 'Hello World';
           |   SET $cnt = $cnt + 1;
           |END
           |WHILE $cnt < 10
           |""".stripMargin)
      val cnt = $$("cnt")
      assert(results == DoWhile(
        condition = cnt < 10,
        invokable = SQL(
          Println("Hello World"),
          SetLocalVariable(cnt.name, cnt + 1)
        )
      ))
    }

    it("should support WHILE ... DO") {
      val results = SQLCompiler.compile(
        """|WHILE $cnt < 10 DO
           |BEGIN
           |   PRINTLN 'Hello World';
           |   SET $cnt = $cnt + 1;
           |END;
           |""".stripMargin)
      val cnt = $$("cnt")
      assert(results == WhileDo(
        condition = cnt < 10,
        invokable = SQL(
          Println("Hello World"),
          SetLocalVariable(cnt.name, cnt + 1)
        )
      ))
    }

  }

}
