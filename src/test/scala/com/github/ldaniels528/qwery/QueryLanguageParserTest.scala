package com.github.ldaniels528.qwery

import org.scalatest.FunSpec

/**
  * Query Language Parser Test
  * @author lawrence.daniels@gmail.com
  */
class QueryLanguageParserTest extends FunSpec {

  describe("QueryLanguageParser") {

    it("parses reference to a CVS file into a query object") {
      val languageParser = new QueryCompiler()
      val query = languageParser(
        """
          |SELECT * FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)

      info(s"query: $query")
    }

  }

}
