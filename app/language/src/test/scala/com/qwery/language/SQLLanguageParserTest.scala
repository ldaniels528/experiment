package com.qwery.language

import com.qwery.models.Insert.{Into, Overwrite}
import com.qwery.models._
import com.qwery.models.expressions.{Field, VariableRef}
import org.scalatest.FunSpec

/**
  * SQL Language Parser Test
  * @author lawrence.daniels@gmail.com
  */
class SQLLanguageParserTest extends FunSpec {

  describe(classOf[SQLLanguageParser].getSimpleName) {
    import com.qwery.models.expressions.Expression.Implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    // create the SQL language parser
    val sqlLanguageParser = new SQLLanguageParser {}

    it("should support BEGIN ... END statements") {
      // test type 1
      val results1 = sqlLanguageParser.parse(
        """|BEGIN
           |  PRINT 'Hello World'
           |END""".stripMargin)
      assert(results1 == SQL(Console.Print("Hello World")))

      // test type 2
      val results2 = sqlLanguageParser.parse(
        """|{
           |  PRINT 'Hello World'
           |}""".stripMargin)
      assert(results2 == SQL(Console.Print("Hello World")))
    }

    it("should support CALL statements") {
      val results = sqlLanguageParser.parse("CALL computeArea(length, width)")
      assert(results == CallProcedure(name = "computeArea", args = List("length", "width").map(Field.apply)))
    }

    it("should support CREATE EXTERNAL TABLE statements") {
      val results = sqlLanguageParser.parse(
        """|CREATE EXTERNAL TABLE Customers (customer_id STRING, name STRING, address STRING, ingestion_date LONG)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |ROW FORMAT DELIMITED
           |FIELDS TERMINATED BY ','
           |STORED AS INPUTFORMAT 'CSV'
           |OUTPUTFORMAT 'CSV'
           |LOCATION './dataSets/customers/csv/'
           |""".stripMargin)
      assert(results == Create(Table(name = "Customers",
        columns = List("customer_id STRING", "name STRING", "address STRING", "ingestion_date LONG").map(Column.apply),
        fieldDelimiter = ",",
        fieldTerminator = None,
        inputFormat = StorageFormats.CSV,
        outputFormat = StorageFormats.CSV,
        location = "./dataSets/customers/csv/"
      )))
    }

    it("should support CREATE FUNCTION") {
      val results = sqlLanguageParser.parse("CREATE FUNCTION myFunc AS 'com.qwery.udf.MyFunc'")
      assert(results == Create(UserDefinedFunction(name = "myFunc", `class` = "com.qwery.udf.MyFunc", jar = None)))
    }

    it("should support CREATE LOGICAL TABLE statements") {
      val results = sqlLanguageParser.parse(
        """|CREATE LOGICAL TABLE SpecialSecurities (symbol STRING, lastSale DOUBLE)
           |FROM VALUES ('AAPL', 202.11), ('AMD', 23.50), ('GOOG', 765.33), ('AMZN', 699.01)""".stripMargin)
      assert(results == Create(LogicalTable(
        name = "SpecialSecurities",
        columns = List("symbol STRING", "lastSale DOUBLE").map(Column.apply),
        source = Insert.Values(List(List("AAPL", 202.11), List("AMD", 23.50), List("GOOG", 765.33), List("AMZN", 699.01)))
      )))
    }

    it("should support CREATE PROCEDURE statements") {
      val results = sqlLanguageParser.parse(
        """|CREATE PROCEDURE testInserts(industry STRING) AS
           |  RETURN (
           |    SELECT Symbol, Name, Sector, Industry, `Summary Quote`
           |    FROM Customers
           |    WHERE Industry = @industry
           |  )
           |""".stripMargin)
      assert(results == Create(Procedure(name = "testInserts",
        params = List("industry STRING").map(Column.apply),
        code = Return(Select(
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          from = TableRef.parse("Customers"),
          where = Field("Industry") === VariableRef(name = "industry")
        ))
      )))
    }

    it("should support CREATE TABLE statements") {
      val results = sqlLanguageParser.parse(
        """|CREATE TABLE Customers (customer_uid UUID, name STRING, address STRING, ingestion_date LONG)
           |PARTITIONED BY (year STRING, month STRING, day STRING)
           |ROW FORMAT DELIMITED
           |FIELDS TERMINATED BY ','
           |STORED AS INPUTFORMAT 'CSV'
           |OUTPUTFORMAT 'CSV'
           |LOCATION './dataSets/customers/csv/'""".stripMargin)
      assert(results == Create(Table(name = "Customers",
        columns = List("customer_uid UUID", "name STRING", "address STRING", "ingestion_date LONG").map(Column.apply),
        fieldDelimiter = ",",
        fieldTerminator = None,
        inputFormat = StorageFormats.CSV,
        outputFormat = StorageFormats.CSV,
        location = "./dataSets/customers/csv/"
      )))
    }

    it("should support CREATE TEMPORARY FUNCTION") {
      val results = sqlLanguageParser.parse(
        """|CREATE TEMPORARY FUNCTION myFunc AS 'com.qwery.udf.MyFunc'
           |USING JAR '/home/ldaniels/shocktrade/jars/shocktrade-0.8.jar'""".stripMargin)
      assert(results == Create(UserDefinedFunction(
        name = "myFunc",
        `class` = "com.qwery.udf.MyFunc",
        jar = "/home/ldaniels/shocktrade/jars/shocktrade-0.8.jar"
      )))
    }

    it("should support CREATE VIEW statements") {
      val results = sqlLanguageParser.parse(
        """|CREATE VIEW OilAndGas AS
           |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)
      assert(results == Create(View(name = "OilAndGas",
        query = Select(
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          from = TableRef.parse("Customers"),
          where = Field("Industry") === "Oil/Gas Transmission"
        ))))
    }

    it("should support DEBUG, ERROR, INFO, PRINT and WARN statements") {
      import Console._
      val tests = Map[String, (String => Invokable, String)](elems =
        "DEBUG" -> (Debug, "This is a debug message"),
        "ERROR" -> (Error, "This is an error message"),
        "INFO" -> (Info, "This is an informational message"),
        "PRINT" -> (Print, "This message will be printed to STDOUT"),
        "WARN" -> (Warn, "This is a warning message"))
      tests foreach { case (command, (opCode, message)) =>
        val results = sqlLanguageParser.parse(s"$command '$message'")
        assert(results == opCode(message))
      }
    }

    it("should support INCLUDE statements") {
      val results = sqlLanguageParser.parse("INCLUDE 'models/securities.sql'")
      assert(results == Include(List("models/securities.sql")))
    }

    it("should support INSERT statements without explicitly defined fields") {
      val results = sqlLanguageParser.parse(
        "INSERT INTO Students VALUES ('Fred Flintstone', 35, 1.28), ('Barney Rubble', 32, 2.32)")
      assert(results == Insert(Into(TableRef.parse("Students")), Insert.Values(values = List(
        List("Fred Flintstone", 35.0, 1.28),
        List("Barney Rubble", 32.0, 2.32)
      ))))
    }

    it("should support INSERT-INTO-SELECT statements") {
      val results = sqlLanguageParser.parse(
        """|INSERT INTO TABLE OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)
      val fields = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry).map(s => Field(s.name))
      assert(results == Insert(Into(TableRef.parse("OilGasSecurities")),
        Select(
          fields = fields,
          from = TableRef.parse("Securities"),
          where = Field("Industry") === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support INSERT-INTO-VALUES statements") {
      val results = sqlLanguageParser.parse(
        """|INSERT INTO TABLE OilGasSecurities (Symbol, Name, Sector, Industry, LastSale)
           |VALUES
           |  ('AAPL', 'Apple, Inc.', 'Technology', 'Computers', 203.45),
           |  ('AMD', 'American Micro Devices, Inc.', 'Technology', 'Computers', 22.33)""".stripMargin)
      val fields = List('Symbol, 'Name, 'Sector, 'Industry, 'LastSale).map(s => Field(s.name))
      assert(results == Insert(Into(TableRef.parse("OilGasSecurities")),
        Insert.Values(
          values = List(
            List("AAPL", "Apple, Inc.", "Technology", "Computers", 203.45),
            List("AMD", "American Micro Devices, Inc.", "Technology", "Computers", 22.33)
          )),
        fields = fields
      ))
    }

    it("should support INSERT-INTO-LOCATION-SELECT statements") {
      val results = sqlLanguageParser.parse(
        """|INSERT INTO LOCATION '/dir/subdir' (Symbol, Name, Sector, Industry, LastSale)
           |SELECT Symbol, Name, Sector, Industry, LastSale
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)
      val fields = List('Symbol, 'Name, 'Sector, 'Industry, 'LastSale).map(s => Field(s.name))
      assert(results == Insert(Into(LocationRef("/dir/subdir")),
        Select(
          fields = fields,
          from = TableRef.parse("Securities"),
          where = Field("Industry") === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support INSERT-OVERWRITE-SELECT statements") {
      val results = sqlLanguageParser.parse(
        """|INSERT OVERWRITE TABLE OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)
      val fields = List('Symbol, 'Name, 'LastSale, 'MarketCap, 'IPOyear, 'Sector, 'Industry).map(s => Field(s.name))
      assert(results == Insert(Overwrite(TableRef.parse("OilGasSecurities")),
        Select(
          fields = fields,
          from = TableRef.parse("Securities"),
          where = Field("Industry") === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support INSERT-OVERWRITE-VALUES statements") {
      val results = sqlLanguageParser.parse(
        """|INSERT OVERWRITE TABLE OilGasSecurities (Symbol, Name, Sector, Industry, LastSale)
           |VALUES
           |  ('AAPL', 'Apple, Inc.', 'Technology', 'Computers', 203.45),
           |  ('AMD', 'American Micro Devices, Inc.', 'Technology', 'Computers', 22.33)""".stripMargin)
      val fields = List('Symbol, 'Name, 'Sector, 'Industry, 'LastSale).map(s => Field(s.name))
      assert(results == Insert(Overwrite(TableRef.parse("OilGasSecurities")),
        Insert.Values(values = List(
          List("AAPL", "Apple, Inc.", "Technology", "Computers", 203.45),
          List("AMD", "American Micro Devices, Inc.", "Technology", "Computers", 22.33)
        )),
        fields = fields
      ))
    }

    it("should support INSERT-OVERWRITE-LOCATION-SELECT statements") {
      val results = sqlLanguageParser.parse(
        """|INSERT OVERWRITE LOCATION '/dir/subdir' (Symbol, Name, Sector, Industry, LastSale)
           |SELECT Symbol, Name, Sector, Industry, LastSale
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)
      val fields = List('Symbol, 'Name, 'Sector, 'Industry, 'LastSale).map(s => Field(s.name))
      assert(results == Insert(Overwrite(LocationRef("/dir/subdir")),
        Select(
          fields = fields,
          from = TableRef.parse("Securities"),
          where = Field("Industry") === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support MAIN PROGRAM statements") {
      val results = sqlLanguageParser.parse(
        """|MAIN PROGRAM 'Oil_Companies'
           |  WITH ARGUMENTS AS @args
           |  WITH ENVIRONMENT AS @env
           |  WITH HIVE SUPPORT
           |  WITH STREAM PROCESSING
           |AS
           |BEGIN
           |  /* does nothing */
           |END
           |""".stripMargin)
      assert(results == MainProgram(name = "Oil_Companies", code = SQL(), hiveSupport = true, streaming = true))
    }

    it("should support SELECT statements") {
      val results = sqlLanguageParser.parse(
        """|SELECT C.Symbol, C.Name, C.Sector, C.Industry, `C.Summary Quote`
           |FROM Customers AS C
           |WHERE C.Industry = 'Oil/Gas Transmission'
           |ORDER BY C.Symbol DESC
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.Symbol", "C.Name", "C.Sector", "C.Industry", "C.Summary Quote").map(Field.apply),
        from = TableRef.parse("C.Customers"),
        where = Field("C.Industry") === "Oil/Gas Transmission",
        orderBy = List(OrderColumn(name = "Symbol", ascending = false).as("C"))
      ))
    }

    it("should support SELECT w/INNER JOIN statements") {
      val results = sqlLanguageParser.parse(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers as C
           |INNER JOIN CustomerAddresses as CA ON CA.customerId = C.customerId
           |INNER JOIN Addresses as A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(Field.apply),
        from = TableRef.parse("C.Customers"),
        joins = List(
          Join(TableRef.parse("CA.CustomerAddresses"), Field("CA.customerId") === Field("C.customerId"), JoinTypes.INNER),
          Join(TableRef.parse("A.Addresses"), Field("A.addressId") === Field("CA.addressId"), JoinTypes.INNER)
        ),
        where = Field("C.firstName") === "Lawrence" && Field("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/FULL OUTER JOIN statements") {
      val results = sqlLanguageParser.parse(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers as C
           |FULL OUTER JOIN CustomerAddresses as CA ON CA.customerId = C.customerId
           |FULL OUTER JOIN Addresses as A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(Field.apply),
        from = TableRef.parse("C.Customers"),
        joins = List(
          Join(TableRef.parse("CA.CustomerAddresses"), Field("CA.customerId") === Field("C.customerId"), JoinTypes.FULL_OUTER),
          Join(TableRef.parse("A.Addresses"), Field("A.addressId") === Field("CA.addressId"), JoinTypes.FULL_OUTER)
        ),
        where = Field("C.firstName") === "Lawrence" && Field("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/LEFT OUTER JOIN statements") {
      val results = sqlLanguageParser.parse(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers as C
           |LEFT OUTER JOIN CustomerAddresses as CA ON CA.customerId = C.customerId
           |LEFT OUTER JOIN Addresses as A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(Field.apply),
        from = TableRef.parse("C.Customers"),
        joins = List(
          Join(TableRef.parse("CA.CustomerAddresses"), Field("CA.customerId") === Field("C.customerId"), JoinTypes.LEFT_OUTER),
          Join(TableRef.parse("A.Addresses"), Field("A.addressId") === Field("CA.addressId"), JoinTypes.LEFT_OUTER)
        ),
        where = Field("C.firstName") === "Lawrence" && Field("C.lastName") === "Daniels"
      ))
    }

    it("should support SELECT w/RIGHT OUTER JOIN statements") {
      val results = sqlLanguageParser.parse(
        """|SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |FROM Customers as C
           |RIGHT OUTER JOIN CustomerAddresses as CA ON CA.customerId = C.customerId
           |RIGHT OUTER JOIN Addresses as A ON A.addressId = CA.addressId
           |WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels'
           |""".stripMargin)
      assert(results == Select(
        fields = List("C.id", "C.firstName", "C.lastName", "A.city", "A.state", "A.zipCode").map(Field.apply),
        from = TableRef.parse("C.Customers"),
        joins = List(
          Join(TableRef.parse("CA.CustomerAddresses"), Field("CA.customerId") === Field("C.customerId"), JoinTypes.RIGHT_OUTER),
          Join(TableRef.parse("A.Addresses"), Field("A.addressId") === Field("CA.addressId"), JoinTypes.RIGHT_OUTER)
        ),
        where = Field("C.firstName") === "Lawrence" && Field("C.lastName") === "Daniels"
      ))
    }

    it("should support SET statements") {
      val results = sqlLanguageParser.parse(
        """|{
           |  SET @customers = (
           |    SELECT Symbol, Name, Sector, Industry, `Summary Quote`
           |    FROM Customers
           |    WHERE Industry = 'Oil/Gas Transmission'
           |    ORDER BY Symbol ASC
           |  )
           |}
           |""".stripMargin)
      assert(results == SQL(Assign(variable = VariableRef(name = "customers"),
        Select(
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          from = TableRef.parse("Customers"),
          where = Field("Industry") === "Oil/Gas Transmission",
          orderBy = List(OrderColumn(name = "Symbol"))
        ))))
    }

    it("should support SHOW statements") {
      val results = sqlLanguageParser.parse("SHOW @theResults LIMIT 5")
      assert(results == Show(rows = VariableRef(name = "theResults"), limit = 5))
    }

    it("should support UNION statements") {
      val results = sqlLanguageParser.parse(
        """|SELECT Symbol, Name, Sector, Industry, `Summary Quote`
           |FROM Customers
           |WHERE Industry = 'Oil/Gas Transmission'
           |UNION
           |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
           |FROM Customers
           |WHERE Industry = 'Computer Manufacturing'
           |""".stripMargin)
      assert(results == Union(
        Select(
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          from = TableRef.parse("Customers"),
          where = Field("Industry") === "Oil/Gas Transmission"
        ),
        Select(
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          from = TableRef.parse("Customers"),
          where = Field("Industry") === "Computer Manufacturing"
        )))
    }

    it("should support UPDATE statements") {
      val results = sqlLanguageParser.parse(
        """|UPDATE Companies
           |SET Symbol = 'AAPL', Name = 'Apple, Inc.', Sector = 'Technology', Industry = 'Computers', LastSale = 203.45
           |WHERE Symbol = 'AAPL'
           |""".stripMargin)
      assert(results == Update(
        table = TableRef.parse("Companies"),
        assignments = Seq(
          "Symbol" -> "AAPL", "Name" -> "Apple, Inc.",
          "Sector" -> "Technology", "Industry" -> "Computers", "LastSale" -> 203.45
        ),
        where = Field("Symbol") === "AAPL"
      ))
    }

  }

}
