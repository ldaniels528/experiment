package com.github.ldaniels528.qwery

import org.scalatest.FunSpec

/**
  * Query Compiler Test
  * @author lawrence.daniels@gmail.com
  */
class QueryCompilerParserTest extends FunSpec {

  describe("QueryCompiler") {

    it("parses reference to a CVS file into an executable object") {
      val compiler = new QueryCompiler()
      val query = compiler(
        """
          |SELECT * FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)

      info(s"query: $query")
    }

  }

}
