package com.qwery.language

import com.qwery.language.SQLDecompiler.implicits._
import com.qwery.models.Console
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
  * SQL Decompiler Test Suite
  */
class SQLDecompilerTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[SQLDecompiler].getSimpleName) {

    it("should support ALTER TABLE .. ADD") {
      verify("ALTER TABLE stocks ADD comments TEXT")
    }

    it("should support ALTER TABLE .. ADD COLUMN .. DEFAULT") {
      verify("ALTER TABLE stocks ADD COLUMN comments TEXT DEFAULT 'N/A'")
    }

    it("should support ALTER TABLE .. APPEND") {
      verify("ALTER TABLE stocks APPEND comments TEXT DEFAULT 'N/A'")
    }

    it("should support ALTER TABLE .. APPEND COLUMN") {
      verify("ALTER TABLE stocks APPEND COLUMN comments TEXT")
    }

    it("should support ALTER TABLE .. DROP") {
      verify("ALTER TABLE stocks DROP comments")
    }

    it("should support ALTER TABLE .. DROP COLUMN") {
      verify("ALTER TABLE stocks DROP COLUMN comments")
    }

    it("should support ALTER TABLE .. PREPEND") {
      verify("ALTER TABLE stocks PREPEND comments TEXT")
    }

    it("should support ALTER TABLE .. PREPEND COLUMN") {
      verify("ALTER TABLE stocks PREPEND COLUMN comments TEXT")
    }

    it("should support ALTER TABLE .. RENAME") {
      verify("ALTER TABLE stocks RENAME comments AS remarks")
    }

    it("should support ALTER TABLE .. RENAME COLUMN") {
      verify("ALTER TABLE stocks RENAME COLUMN comments AS remarks")
    }

    it("should support ALTER TABLE .. ADD/DROP") {
      verify("ALTER TABLE stocks ADD comments TEXT DROP remarks")
    }

    it("should support ALTER TABLE .. ADD COLUMN/DROP COLUMN") {
      verify("ALTER TABLE stocks ADD COLUMN comments TEXT DROP COLUMN remarks")
    }

    it("should support BEGIN .. END") {
      verify(
        """|BEGIN
           |  PRINT 'Hello ';
           |  PRINTLN 'World';
           |END
           |""".stripMargin)
    }

    it("should support { .. }") {
      verify(
        """|{
           |  INFO 'Hello World'
           |}
           |""".stripMargin)
    }

    it("should support CALL") {
      verify("CALL computeArea(length, width)")
    }

    it("should support CREATE EXTERNAL TABLE") {
      verify(
        """|CREATE EXTERNAL TABLE Customers (customer_uid UUID, name STRING, address STRING, ingestion_date LONG)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |STORED AS INPUTFORMAT 'JSON' OUTPUTFORMAT 'JSON'
           |LOCATION './dataSets/customers/json/'
           |""".stripMargin)
    }

    it("should support CREATE EXTERNAL TABLE w/COMMENT") {
      verify(
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
    }

    it("should support CREATE EXTERNAL TABLE .. WITH") {
      verify(
        """|CREATE EXTERNAL TABLE Customers (customer_uid UUID, name STRING, address STRING, ingestion_date LONG)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |ROW FORMAT DELIMITED
           |FIELDS TERMINATED BY ','
           |STORED AS INPUTFORMAT 'CSV'
           |WITH HEADERS ON
           |WITH NULL VALUES AS 'n/a'
           |LOCATION './dataSets/customers/csv/'
           |""".stripMargin)
    }

    it("should support CREATE EXTERNAL TABLE .. PARTITIONED BY") {
      verify(
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
    }

    it("should support CREATE FUNCTION") {
      verify("CREATE FUNCTION myFunc AS 'com.qwery.udf.MyFunc'")
    }

    it("should support CREATE FUNCTION .. USING JAR") {
      verify(
        """|CREATE FUNCTION myFunc AS 'com.qwery.udf.MyFunc'
           |USING JAR '/home/ldaniels/shocktrade/jars/shocktrade-0.8.jar'
           |""".stripMargin)
    }

    it("should support CREATE INDEX") {
      verify(
        """|CREATE INDEX stocks_symbol ON stocks (symbol)
           |""".stripMargin)
    }

    it("should support CREATE PROCEDURE") {
      verify(
        """|CREATE PROCEDURE testInserts(industry STRING) AS
           |  RETURN (
           |    SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |    FROM Customers
           |    WHERE Industry = $industry
           |  )
           |""".stripMargin)
    }

    it("should support CREATE TABLE w/ENUM") {
      verify(
        s"""|CREATE TABLE Stocks (
            |  symbol STRING,
            |  exchange STRING AS ENUM ('AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHEROTC'),
            |  lastSale DOUBLE,
            |  lastSaleTime DATE
            |)
            |""".stripMargin
      )
    }

    it("should support CREATE TABLE with inline VALUES") {
      verify(
        """|CREATE TABLE SpecialSecurities (symbol STRING, lastSale DOUBLE)
           |FROM VALUES ('AAPL', 202.11), ('AMD', 23.50), ('GOOG', 765.33), ('AMZN', 699.01)
           |""".stripMargin)
    }

    it("should support CREATE TABLE with subquery") {
      verify(
        """|CREATE TABLE SpecialSecurities (symbol STRING, lastSale DOUBLE)
           |FROM (
           |    SELECT symbol, lastSale
           |    FROM Securities
           |    WHERE useCode = 'SPECIAL'
           |)
           |""".stripMargin)
    }

    it("should support CREATE TYPE .. AS ENUM") {
      verify(
        """|CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')
           |""".stripMargin)
    }

    it("should support CREATE VIEW") {
      verify(
        """|create view if not exists tickers as
           |select
           |   symbol as ticker,
           |   exchange as market,
           |   lastSale,
           |   round(lastSale, 1) as roundedLastSale,
           |   lastSaleTime
           |from securities
           |order by lastSale desc
           |limit 5
           |""".stripMargin
      )
    }

    it("should support CREATE VIEW .. WITH COMMENT") {
      verify(
        """|CREATE VIEW IF NOT EXISTS OilAndGas
           |WITH COMMENT 'AMEX Stock symbols sorted by last sale'
           |AS
           |SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
    }

    it("should support DECLARE") {
      verify("DECLARE $customerId INTEGER")
    }

    it("should support DELETE") {
      verify("DELETE FROM todo_list where item_id = 1238 LIMIT 25")
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
        verify(s"$command '$message'")
      }
    }

    it("should support FOR .. IN") {
      verify(
        """|FOR $item IN (SELECT symbol, lastSale FROM Securities WHERE naics = '12345') {
           |  PRINT '${item.symbol} is ${item.lastSale)/share';
           |}
           |""".stripMargin)
    }

    it("should support FOR .. LOOP") {
      verify(
        """|FOR $item IN REVERSE (SELECT symbol, lastSale FROM Securities WHERE naics = '12345')
           |LOOP
           |  PRINT '${item.symbol} is ${item.lastSale)/share';
           |END LOOP;
           |""".stripMargin)
    }

    it("should support INCLUDE") {
      verify("INCLUDE 'models/securities.sql'")
    }

    it("should support INSERT without explicitly defined fields") {
      verify("INSERT INTO Students VALUES ('Fred Flintstone', 35, 1.28), ('Barney Rubble', 32, 2.32)")
    }

    it("should support INSERT-INTO-SELECT") {
      verify(
        """|INSERT INTO TABLE OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
    }

    it("should support INSERT-INTO-VALUES") {
      verify(
        """|INSERT INTO TABLE OilGasSecurities (Symbol, Name, Sector, Industry, LastSale)
           |VALUES
           |  ('AAPL', 'Apple, Inc.', 'Technology', 'Computers', 203.45),
           |  ('AMD', 'American Micro Devices, Inc.', 'Technology', 'Computers', 22.33)
           |""".stripMargin)
    }

    it("should support INSERT-OVERWRITE-SELECT") {
      verify(
        """|INSERT OVERWRITE TABLE OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
    }

    it("should support INSERT-OVERWRITE-VALUES") {
      verify(
        """|INSERT OVERWRITE TABLE OilGasSecurities (Symbol, Name, Sector, Industry, LastSale)
           |VALUES
           |  ('AAPL', 'Apple, Inc.', 'Technology', 'Computers', 203.45),
           |  ('AMD', 'American Micro Devices, Inc.', 'Technology', 'Computers', 22.33)
           |""".stripMargin)
    }

    it("should support SELECT without a FROM clause") {
      verify("SELECT `$$DATA_SOURCE_ID` AS DATA_SOURCE_ID")
    }

    it("should support SELECT function call") {
      verify("SELECT symbol, customFx(lastSale) FROM Securities WHERE naics = '12345'")
    }

    it("should support SELECT DISTINCT") {
      verify("SELECT DISTINCT Ticker_Symbol FROM Securities WHERE Industry = 'Oil/Gas Transmission'")
    }

    it("should support SELECT .. INTO") {
      verify(
        """|SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |INTO TABLE OilGasSecurities
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
    }

    it("should support SELECT .. EXISTS(..)") {
      verify(
        """|SELECT * FROM Departments WHERE EXISTS(SELECT employee_id FROM Employees WHERE role = 'MANAGER')
           |""".stripMargin)
    }

    it("should support SELECT .. GROUP BY fields") {
      verify(
        """|SELECT Sector, Industry, AVG(LastSale) AS LastSale, COUNT(*) AS total, COUNT(DISTINCT(*)) AS distinctTotal
           |FROM Customers
           |GROUP BY Sector, Industry
           |""".stripMargin)
    }

    it("should support SELECT .. GROUP BY indices") {
      verify(
        """|SELECT Sector, Industry, AVG(LastSale) AS LastSale, COUNT(*) AS total, COUNT(DISTINCT(*)) AS distinctTotal
           |FROM Customers
           |GROUP BY 1, 2
           |""".stripMargin)
    }

    it("should support SELECT .. LIMIT n") {
      verify(
        """|SELECT Symbol, Name, Sector, Industry
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |LIMIT 100
           |""".stripMargin)
    }

    it("should support SELECT .. ORDER BY") {
      verify(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |ORDER BY Symbol DESC, Name ASC
           |""".stripMargin)
    }

    it("should support SELECT .. OVER") {
      verify(
        """|SELECT PAT_ID, DEPT_ID, INS_AMT,
           |MIN(INS_AMT) OVER (PARTITION BY DEPT_ID ORDER BY DEPT_ID ASC) AS MIN_INS_AMT,
           |MAX(INS_AMT) OVER (PARTITION BY DEPT_ID ORDER BY DEPT_ID ASC) AS MAX_INS_AMT
           |FROM PATIENT
           |""".stripMargin
      )
    }

    it("should support SELECT .. OVER w/RANGE") {
      verify(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote, LastSale,
           |       MEAN(LastSale) OVER (
           |          PARTITION BY Symbol
           |          ORDER BY TradeDate ASC
           |          RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
           |       ) AS LastSaleMean
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
    }

    it("should support SELECT .. OVER w/ROWS") {
      verify(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote, LastSale,
           |       MEAN(LastSale) OVER (
           |          PARTITION BY Symbol
           |          ORDER BY TradeDate ASC
           |          ROWS BETWEEN INTERVAL 7 DAYS FOLLOWING AND CURRENT ROW
           |       ) AS LastSaleMean
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
    }

    it("should support SELECT .. OVER w/ROWS UNBOUNDED") {
      verify(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote, LastSale,
           |       MEAN(LastSale) OVER (
           |          PARTITION BY Symbol
           |          ORDER BY TradeDate ASC
           |          ROWS UNBOUNDED FOLLOWING
           |       ) AS LastSaleMean
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin)
    }

    it("should support SELECT .. EXCEPT") {
      verify(
        s"""|SELECT Symbol, Name, Sector, Industry, SummaryQuote
            |FROM Customers
            |WHERE Industry = 'Oil/Gas Transmission'
            |   EXCEPT
            |SELECT Symbol, Name, Sector, Industry, SummaryQuote
            |FROM Customers
            |WHERE Industry = 'Computer Manufacturing'
            |""".stripMargin)
    }

    it("should support SELECT .. INTERSECT") {
      verify(
        s"""|SELECT Symbol, Name, Sector, Industry, SummaryQuote
            |FROM Customers
            |WHERE Industry = 'Oil/Gas Transmission'
            |   INTERSECT
            |SELECT Symbol, Name, Sector, Industry, SummaryQuote
            |FROM Customers
            |WHERE Industry = 'Computer Manufacturing'
            |""".stripMargin)
    }

    it("should support SELECT .. UNION") {
      Seq("ALL", "DISTINCT", "") foreach { modifier =>
        verify(
          s"""|SELECT Symbol, Name, Sector, Industry, SummaryQuote
              |FROM Customers
              |WHERE Industry = 'Oil/Gas Transmission'
              |   UNION $modifier
              |SELECT Symbol, Name, Sector, Industry, SummaryQuote
              |FROM Customers
              |WHERE Industry = 'Computer Manufacturing'
              |""".stripMargin)
      }
    }

    it("should support SELECT .. WHERE BETWEEN") {
      verify(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE IPOYear BETWEEN '2000' AND '2019'
           |""".stripMargin)
    }

    it("should support SELECT .. WHERE IN (..)") {
      verify(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers
           |WHERE IPOYear IN ('2000', '2001', '2003', '2008', '2019')
           |""".stripMargin)
    }

    it("should support SELECT .. WHERE IN (SELECT ..)") {
      verify(
        """|SELECT Symbol, Name, Sector, Industry, SummaryQuote
           |FROM Customers AS C
           |WHERE IPOYear IN (SELECT `Year` FROM EligibleYears)
           |""".stripMargin)
    }

    it("should support SELECT w/CROSS JOIN") {
      verify(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |CROSS JOIN CustomerAddresses AS CA ON CA.customerId = C.customerId
           |CROSS JOIN Addresses AS A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
    }

    it("should support SELECT w/INNER JOIN") {
      verify(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |INNER JOIN CustomerAddresses AS CA ON CA.customerId = C.customerId
           |INNER JOIN Addresses AS A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
    }

    it("should support SELECT w/FULL OUTER JOIN") {
      verify(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |FULL OUTER JOIN CustomerAddresses AS CA ON CA.customerId = C.customerId
           |FULL OUTER JOIN Addresses AS A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
    }

    it("should support SELECT w/LEFT OUTER JOIN") {
      verify(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |LEFT OUTER JOIN CustomerAddresses AS CA ON CA.customerId = C.customerId
           |LEFT OUTER JOIN Addresses AS A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
    }

    it("should support SELECT w/RIGHT OUTER JOIN") {
      verify(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |RIGHT OUTER JOIN CustomerAddresses AS CA ON CA.customerId = C.customerId
           |RIGHT OUTER JOIN Addresses AS A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
    }

    it("should support SELECT w/JOIN .. USING") {
      verify(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers AS C
           |JOIN CustomerAddresses AS CA USING customerId
           |JOIN Addresses AS A USING addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
    }

    it("should support SET variable") {
      verify("SET $customers = $customers + 1")
    }

    it("should support SET row variable") {
      verify(
        """|SET @securities = (
           |  SELECT Symbol, Name, Sector, Industry, `Summary Quote`
           |  FROM Securities
           |  WHERE Industry = 'Oil/Gas Transmission'
           |  ORDER BY Symbol ASC
           |)
           |""".stripMargin)
    }

    it("should support SHOW") {
      verify("SHOW @theResults LIMIT 5")
    }

    it("should support TRUNCATE") {
      verify("TRUNCATE stocks")
    }

    it("should support UPDATE") {
      verify(
        """|UPDATE Companies
           |SET Symbol = 'AAPL', Name = 'Apple, Inc.', Sector = 'Technology', Industry = 'Computers', LastSale = 203.45
           |WHERE Symbol = 'AAPL'
           |LIMIT 25
           |""".stripMargin)
    }

    it("should support DO ... WHILE") {
      verify(
        """|DO
           |BEGIN
           |   PRINTLN 'Hello World';
           |   SET $cnt = $cnt + 1;
           |END
           |WHILE $cnt < 10
           |""".stripMargin)
    }

    it("should support WHILE .. DO") {
      verify(
        """|WHILE $cnt < 10 DO
           |BEGIN
           |   PRINTLN 'Hello World';
           |   SET $cnt = $cnt + 1;
           |END;
           |""".stripMargin)
    }

  }

  def verify(expectedSQL: String): Assertion = {
    logger.info(s"expected: $expectedSQL")
    val expected = SQLLanguageParser.parse(expectedSQL)
    val actualSQL = expected.toSQL
    logger.info(s"actual: $actualSQL")
    val actual = SQLLanguageParser.parse(actualSQL)
    assert(expected == actual)
  }

}
