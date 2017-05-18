package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.Field
import com.github.ldaniels528.qwery.ops.Implicits._
import org.scalatest.FunSpec

/**
  * Template Parser Tests
  * @author lawrence.daniels@gmail.com
  */
class SQLTemplateParserTest extends FunSpec {

  describe("TemplateParser") {

    it("should extract values from SELECT queries") {
      val query =
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM 'companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'
          |LIMIT 5""".stripMargin

      val templateParser = SQLTemplateParser(query)
      val templateParams = templateParser.process("SELECT @{fields} FROM @source ?WHERE ?@&{condition} ?LIMIT ?@limit")
      assert(templateParams == SQLTemplateParams(
        atoms = Map("source" -> "companylist.csv", "limit" -> "5"),
        conditions = Map("condition" -> (Field("Industry") === "Oil/Gas Transmission")),
        expressions = Map("fields" -> List("Symbol", "Name", "Sector", "Industry", "LastSale", "MarketCap").map(Field.apply))
      ))
    }

    it("should extract values from INSERT-VALUES statements") {
      val query =
        """
          |INSERT INTO 'test3.csv' (Symbol, Sector, Industry, LastSale)
          |VALUES ('ACU', 'Capital Goods', 'Industrial Machinery/Components', 29)
          |VALUES ('EMX', 'Basic Industries', 'Precious Metals', 0.828)""".stripMargin

      val tokenStream = TokenStream(TokenIterator(query))
      val templateParser = new SQLTemplateParser(tokenStream)
      val templateParams = templateParser.process("INSERT INTO @target ( @(fields) ) {{valueSet VALUES ( @{values} ) }}")
      assert(templateParams == SQLTemplateParams(
        atoms = Map("target" -> "test3.csv"),
        fields = Map("fields" -> List("Symbol", "Sector", "Industry", "LastSale").map(Field.apply)),
        repeatedSets = Map("valueSet" -> List(
          SQLTemplateParams(expressions = Map("values" -> List("ACU", "Capital Goods", "Industrial Machinery/Components", 29.0))),
          SQLTemplateParams(expressions = Map("values" -> List("EMX", "Basic Industries", "Precious Metals", 0.828)))
        ))
      ))
    }

  }
}
