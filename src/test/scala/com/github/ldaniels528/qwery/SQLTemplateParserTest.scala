package com.github.ldaniels528.qwery

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
      val values = templateParser.extract("SELECT @{fields} FROM @source ?WHERE ?@&{condition} ?LIMIT ?@limit")
      info(s"values: $values")
    }

    it("should extract values from INSERT-VALUES statements") {
      val query =
        """
          |INSERT INTO 'test3.csv' (Symbol, Sector, Industry, LastSale)
          |VALUES ('ACU', 'Capital Goods', 'Industrial Machinery/Components', 29)
          |VALUES ('EMX', 'Basic Industries', 'Precious Metals', 0.828)""".stripMargin

      val tokenStream = TokenStream(TokenIterator(query))
      val templateParser = new SQLTemplateParser(tokenStream)
      val targetAndFields = templateParser.extract("INSERT INTO @target ( @(fields) )")
      var valueSets: List[SQLTemplateParams] = Nil
      while (tokenStream.hasNext) {
        valueSets = templateParser.extract("VALUES ( @{values} )") :: valueSets
      }
      info(s"targetAndFields: $targetAndFields")
      valueSets.foreach(values => info(s"values:          $values"))
    }
  }

}
