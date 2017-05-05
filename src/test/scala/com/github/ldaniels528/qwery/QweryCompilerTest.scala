package com.github.ldaniels528.qwery

import java.io.File

import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.sources._
import org.scalatest.FunSpec

/**
  * Qwery Compiler Test
  * @author lawrence.daniels@gmail.com
  */
class QweryCompilerTest extends FunSpec {

  describe("QweryCompiler") {

    it("should parses a SELECT query and returns the equivalent executable") {
      val compiler = new QweryCompiler()
      val query =
        """
          |SELECT * FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin
      val executable = compiler.compile(query)

      assert(executable ==
        Select(
          source = Some(DelimitedInputSource(new File("./companylist.csv"))),
          fields = List(Field("*")),
          condition = Some(EQ(Field("Industry"), StringValue("Oil/Gas Transmission"))),
          limit = None))
    }

    it("should parses a INSERT-SELECT statement and returns the equivalent executable") {
      val compiler = new QweryCompiler()
      val query =
        """
          |INSERT INTO './test2.csv' (Symbol, Sector, Industry, LastSale)
          |SELECT Symbol, Sector, Industry, LastSale FROM './companylist.csv'
          |WHERE Industry = 'Precious Metals'""".stripMargin
      val executable = compiler.compile(query)

      assert(executable ==
        Insert(
          target = DelimitedOutputSource(new File("./test2.csv")),
          fields = List(Field("Symbol"), Field("Sector"), Field("Industry"), Field("LastSale")),
          source = Select(
            source = Some(DelimitedInputSource(new File("./companylist.csv"))),
            fields = List(Field("Symbol"), Field("Sector"), Field("Industry"), Field("LastSale")),
            condition = Some(EQ(Field("Industry"), StringValue("Precious Metals"))),
            limit = None)))
    }

    it("should parses a INSERT statement and returns the equivalent executable") {
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
          fields = List(Field("Symbol"), Field("Sector"), Field("Industry"), Field("LastSale")),
          source = Modifications(
            fields = List(Field("Symbol"), Field("Sector"), Field("Industry"), Field("LastSale")),
            dataSets = List(
              List("EMX", "Basic Industries", "Precious Metals", 0.828),
              List("ACU", "Capital Goods", "Industrial Machinery/Components", 29.0)))
        ))
    }

  }

}
