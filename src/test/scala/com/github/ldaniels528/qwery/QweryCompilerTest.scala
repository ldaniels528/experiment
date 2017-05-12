package com.github.ldaniels528.qwery

import java.io.File

import com.github.ldaniels528.qwery.ops.Field.AllFields
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.sources._
import org.scalatest.FunSpec

/**
  * Qwery Compiler Test
  * @author lawrence.daniels@gmail.com
  */
class QweryCompilerTest extends FunSpec {

  describe("QweryCompiler") {

    it("should compiles SELECT queries") {
      val compiler = new QweryCompiler()
      val query =
        """
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
          |FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin
      val executable = compiler.compile(query)
      assert(executable ==
        Select(
          source = Some(DelimitedInputSource(new File("./companylist.csv"))),
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          condition = Some(EQ(Field("Industry"), StringValue("Oil/Gas Transmission")))
        ))
    }

    it("should compiles SELECT queries for all (*) fields") {
      val compiler = new QweryCompiler()
      val query =
        """
          |SELECT * FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin
      val executable = compiler.compile(query)
      assert(executable ==
        Select(
          source = Some(DelimitedInputSource(new File("./companylist.csv"))),
          fields = List(AllFields),
          condition = Some(EQ(Field("Industry"), StringValue("Oil/Gas Transmission")))
        ))
    }

    it("should compiles SELECT queries with ORDER BY clauses") {
      val compiler = new QweryCompiler()
      val query =
        """
          |SELECT * FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'
          |ORDER BY Symbol DESC""".stripMargin
      val executable = compiler.compile(query)
      assert(executable ==
        Select(
          source = Some(DelimitedInputSource(new File("./companylist.csv"))),
          fields = List(AllFields),
          condition = Some(EQ(Field("Industry"), StringValue("Oil/Gas Transmission"))),
          sortFields = Option(List(Field("Symbol") -> -1))
        ))
    }

    it("should compiles SELECT queries with GROUP BY and ORDER BY clauses") {
      val compiler = new QweryCompiler()
      val query =
        """
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote` FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'
          |GROUP BY Symbol
          |ORDER BY Symbol DESC""".stripMargin
      val executable = compiler.compile(query)
      assert(executable ==
        Select(
          source = Some(DelimitedInputSource(new File("./companylist.csv"))),
          fields = List("Symbol", "Name", "Sector", "Industry", "Summary Quote").map(Field.apply),
          condition = Some(EQ(Field("Industry"), StringValue("Oil/Gas Transmission"))),
          groupFields = Option(List(Field("Symbol"))),
          sortFields = Option(List(Field("Symbol") -> -1))
        ))
    }

    it("should compiles INSERT-SELECT statements") {
      val compiler = new QweryCompiler()
      val query =
        """
          |INSERT OVERWRITE './test2.csv' (Symbol, Sector, Industry, LastSale)
          |SELECT Symbol, Sector, Industry, LastSale FROM './companylist.csv'
          |WHERE Industry = 'Precious Metals'""".stripMargin
      val executable = compiler.compile(query)
      assert(executable ==
        Insert(
          target = DelimitedOutputSource(new File("./test2.csv")),
          fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
          source = Select(
            source = Some(DelimitedInputSource(new File("./companylist.csv"))),
            fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
            condition = Some(EQ(Field("Industry"), StringValue("Precious Metals"))),
            limit = None),
          hints = Hints(append = false)))
    }

    it("should compiles INSERT statements") {
      val compiler = new QweryCompiler()
      val query =
        """
          |INSERT INTO './test3.csv' (Symbol, Sector, Industry, LastSale)
          |VALUES ('ACU', 'Capital Goods', 'Industrial Machinery/Components', 29)
          |VALUES ('EMX', 'Basic Industries', 'Precious Metals', 0.828)""".stripMargin
      val executable = compiler.compile(query)
      assert(executable ==
        Insert(
          target = DelimitedOutputSource(new File("./test3.csv")),
          fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
          source = InsertValues(
            fields = List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply),
            dataSets = List(
              List("ACU", "Capital Goods", "Industrial Machinery/Components", 29.0).map(Expression.apply),
              List("EMX", "Basic Industries", "Precious Metals", 0.828).map(Expression.apply)
            )),
          hints = Hints(append = true)
        ))
    }

  }

}
