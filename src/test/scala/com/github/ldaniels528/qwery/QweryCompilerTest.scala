package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.Implicits._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.ops.builtins.Case.When
import com.github.ldaniels528.qwery.ops.builtins._
import com.github.ldaniels528.qwery.sources._
import org.scalatest.FunSpec

/**
  * Qwery Compiler Test
  * @author lawrence.daniels@gmail.com
  */
class QweryCompilerTest extends FunSpec {

  describe("QweryCompiler") {

    it("should support execution multi-line statements") {
      val results = QweryCompiler.compileFully("SELECT 1;SELECT 2;SELECT 3").toSeq
      assert(results == Seq(
        Select(fields = List(1)),
        Select(fields = List(2)),
        Select(fields = List(3))
      ))
    }

    it("should support variable assignments") {
      val sql = "SET @x = 1"
      assert(QweryCompiler(sql) == Assignment(VariableRef("x"), 1))
    }

    it("should support variable declarations") {
      val sql = "DECLARE @counter DOUBLE"
      assert(QweryCompiler(sql) == Declare(VariableRef("counter"), "DOUBLE"))
    }

    it("should support CREATE PROCEDURE statements") {
      val sql =
        """
          |CREATE PROCEDURE copyData AS
          |BEGIN
          |   SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |   FROM 'companylist.csv'
          |   WHERE Industry = 'Consumer Specialties'
          |END
        """.stripMargin

      assert(QweryCompiler(sql) == Procedure(name = "copyData", parameters = Nil, executable = CodeBlock(Seq(
        Select(
          fields = List("Symbol", "Name", "Sector", "Industry", "LastSale", "MarketCap").map(Field.apply),
          source = Some(DataResource("companylist.csv")),
          condition = Some(EQ(Field("Industry"), "Consumer Specialties")))
      ))))
    }

    it("should support CREATE VIEW statements") {
      val sql =
        """
          |CREATE VIEW 'OilAndGas' AS
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
          |FROM 'companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin
      assert(QweryCompiler(sql) ==
        View(
          name = "OilAndGas",
          query = Select(
            fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
            source = Some(DataResource("companylist.csv")),
            condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission")))))
    }

    it("should support creating named aliases") {
      val sql = "SELECT 1234 AS number"
      assert(QweryCompiler(sql) ==
        Select(fields = List(NamedExpression(name = "number", 1234))))
    }

    it("should support fixed-length fields") {
      val sql =
        """
          |INSERT INTO 'fixed-data.txt' (Symbol^10, Name^40, Sector^40, Industry^40, LastTrade^10)
          |SELECT Symbol, Name, Sector, Industry, LastSale
          |FROM 'companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'
        """.stripMargin
      info(QweryCompiler(sql).toString)
    }

    it("should support IS NULL conditions") {
      val sql = "SELECT * FROM 'companylist.csv' WHERE exchange IS NULL"
      assert(QweryCompiler(sql) ==
        Select(
          fields = List(AllFields),
          source = Some(DataResource("companylist.csv")),
          condition = Some(IsNull(Field("exchange")))))
    }

    it("should support IS NOT NULL conditions") {
      val sql = "SELECT * FROM 'companylist.csv' WHERE exchange IS NOT NULL"
      assert(QweryCompiler(sql) ==
        Select(
          fields = List(AllFields),
          source = Some(DataResource("companylist.csv")),
          condition = Some(NOT(IsNull(Field("exchange"))))))
    }

    it("should CAST values from one type to another") {
      val sql = "SELECT CAST('1234' AS Double) AS number"
      assert(QweryCompiler(sql) ==
        Select(fields = List(NamedExpression(name = "number", Cast("1234", "Double")))))
    }

    it("should support CASE statement") {
      val sql =
        """
          |SELECT
          |   CASE 'Hello World'
          |     WHEN 'HelloWorld' THEN '1'
          |     WHEN 'Hello' || ' ' || 'World' THEN '2'
          |     ELSE '3'
          |   END AS ItemNo
        """.stripMargin
      assert(QweryCompiler(sql) ==
        Select(fields = List(
          NamedExpression("ItemNo", Case(conditions = List(
            When(EQ("Hello World", "HelloWorld"), "1"),
            When(EQ("Hello World", Concat(Concat("Hello", " "), "World")), "2")),
            otherwise = Some("3"))))
        ))
    }

    it("should compile SELECT queries") {
      val sql =
        """
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
          |FROM 'companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin
      assert(QweryCompiler(sql) ==
        Select(
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          source = Option(DataResource(path = "companylist.csv")),
          condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission"))
        ))
    }

    it("should compile SELECT queries for all (*) fields") {
      val sql =
        """
          |SELECT * FROM 'companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin
      assert(QweryCompiler(sql) ==
        Select(
          fields = List(AllFields),
          source = Option(DataResource(path = "companylist.csv")),
          condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission"))
        ))
    }

    it("should compile SELECT queries with ORDER BY clauses") {
      val sql =
        """
          |SELECT * FROM 'companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'
          |ORDER BY Symbol ASC""".stripMargin
      assert(QweryCompiler(sql) ==
        Select(
          fields = List(AllFields),
          source = Option(DataResource(path = "companylist.csv")),
          condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission")),
          orderedColumns = List(OrderedColumn("Symbol", ascending = true))
        ))
    }

    it("should compile SELECT queries with GROUP BY and ORDER BY clauses") {
      val sql =
        """
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote` FROM 'companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'
          |GROUP BY Symbol
          |ORDER BY Symbol DESC""".stripMargin
      assert(QweryCompiler(sql) ==
        Select(
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          source = Option(DataResource(path = "companylist.csv")),
          condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission")),
          groupFields = List(Field("Symbol")),
          orderedColumns = List(OrderedColumn("Symbol", ascending = false))
        ))
    }

    it("should compile SELECT queries with UNION") {
      val sql =
        """
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
          |FROM 'companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'
          |UNION
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
          |FROM 'companylist.csv'
          |WHERE Industry = 'Integrated oil Companies'""".stripMargin
      assert(QweryCompiler(sql) ==
        Union(
          Select(
            fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
            source = Option(DataResource(path = "companylist.csv")),
            condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission"))
          ),
          Select(
            fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
            source = Option(DataResource(path = "companylist.csv")),
            condition = Some(EQ(Field("Industry"), "Integrated oil Companies"))
          )))
    }

    it("should compile INSERT-SELECT statements") {
      val sql =
        """
          |INSERT OVERWRITE 'test2.csv' (Symbol, Sector, Industry, LastSale)
          |SELECT Symbol, Sector, Industry, LastSale FROM 'companylist.csv'
          |WHERE Industry = 'Precious Metals'""".stripMargin
      assert(QweryCompiler(sql) ==
        Insert(
          fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
          target = DataResource("test2.csv", hints = Some(Hints(
            append = Some(false),
            fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply)))),
          source = Select(
            fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
            source = Option(DataResource(path = "companylist.csv")),
            condition = Some(EQ(Field("Industry"), "Precious Metals")),
            limit = None)
        ))
    }

    it("should compile INSERT statements") {
      val sql =
        """
          |INSERT INTO 'test3.csv' (Symbol, Sector, Industry, LastSale)
          |VALUES ('ACU', 'Capital Goods', 'Industrial Machinery/Components', 29)
          |VALUES ('EMX', 'Basic Industries', 'Precious Metals', 0.828)""".stripMargin
      assert(QweryCompiler(sql) ==
        Insert(
          fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
          target = DataResource("test3.csv", hints = Some(Hints(
            append = Some(true),
            fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply)
          ))),
          source = InsertValues(
            fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
            dataSets = List(
              List[Expression]("ACU", "Capital Goods", "Industrial Machinery/Components", 29.0),
              List[Expression]("EMX", "Basic Industries", "Precious Metals", 0.828)
            ))
        ))
    }

  }

}
