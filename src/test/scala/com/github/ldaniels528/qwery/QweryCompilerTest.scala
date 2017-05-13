package com.github.ldaniels528.qwery

import java.io.File

import com.github.ldaniels528.qwery.ops.builtins.Cast
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.ops.types._
import com.github.ldaniels528.qwery.sources._
import org.scalatest.FunSpec

/**
  * Qwery Compiler Test
  * @author lawrence.daniels@gmail.com
  */
class QweryCompilerTest extends FunSpec {

  describe("QweryCompiler") {

    it("should CAST values from one type to another") {
      val query = "SELECT CAST('1234' AS Double) AS number"
      assert(QweryCompiler(query) ==
        Select(
          fields = List(NamedExpression.alias(name = "number", Cast("1234", "Double")))
        ))
    }

    it("should compiles SELECT queries") {
      val query =
        """
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
          |FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin
      assert(QweryCompiler(query) ==
        Select(
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          source = Some(DelimitedInputSource(new File("./companylist.csv"))),
          condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission"))
        ))
    }

    it("should compiles SELECT queries for all (*) fields") {
      val query =
        """
          |SELECT * FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin
      assert(QweryCompiler(query) ==
        Select(
          fields = List(AllFields),
          source = Some(DelimitedInputSource(new File("./companylist.csv"))),
          condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission"))
        ))
    }

    it("should compiles SELECT queries with ORDER BY clauses") {
      val query =
        """
          |SELECT * FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'
          |ORDER BY Symbol DESC""".stripMargin
      assert(QweryCompiler(query) ==
        Select(
          fields = List(AllFields),
          source = Some(DelimitedInputSource(new File("./companylist.csv"))),
          condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission")),
          sortFields = Option(List(Field("Symbol") -> -1))
        ))
    }

    it("should compiles SELECT queries with GROUP BY and ORDER BY clauses") {
      val query =
        """
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote` FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'
          |GROUP BY Symbol
          |ORDER BY Symbol DESC""".stripMargin
      assert(QweryCompiler(query) ==
        Select(
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          source = Some(DelimitedInputSource(new File("./companylist.csv"))),
          condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission")),
          groupFields = Option(List(Field("Symbol"))),
          sortFields = Option(List(Field("Symbol") -> -1))
        ))
    }

    it("should compiles INSERT-SELECT statements") {
      val query =
        """
          |INSERT OVERWRITE './test2.csv' (Symbol, Sector, Industry, LastSale)
          |SELECT Symbol, Sector, Industry, LastSale FROM './companylist.csv'
          |WHERE Industry = 'Precious Metals'""".stripMargin
      assert(QweryCompiler(query) ==
        Insert(
          fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
          target = DelimitedOutputSource(new File("./test2.csv")),
          source = Select(
            fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
            source = Some(DelimitedInputSource(new File("./companylist.csv"))),
            condition = Some(EQ(Field("Industry"), "Precious Metals")),
            limit = None),
          hints = Hints(append = false)))
    }

    it("should compiles INSERT statements") {
      val query =
        """
          |INSERT INTO './test3.csv' (Symbol, Sector, Industry, LastSale)
          |VALUES ('ACU', 'Capital Goods', 'Industrial Machinery/Components', 29)
          |VALUES ('EMX', 'Basic Industries', 'Precious Metals', 0.828)""".stripMargin
      assert(QweryCompiler(query) ==
        Insert(
          fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
          target = DelimitedOutputSource(new File("./test3.csv")),
          source = InsertValues(
            fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
            dataSets = List(
              List[Expression]("ACU", "Capital Goods", "Industrial Machinery/Components", 29.0),
              List[Expression]("EMX", "Basic Industries", "Precious Metals", 0.828)
            )),
          hints = Hints(append = true)
        ))
    }

  }

}
