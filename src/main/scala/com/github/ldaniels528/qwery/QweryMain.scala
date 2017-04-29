package com.github.ldaniels528.qwery

import com.github.ldaniels528.tabular.Tabular

/**
  * Qwery Main Application
  * @author lawrence.daniels@gmail.com
  */
object QweryMain {

  def main(args: Array[String]) {
    implicit val compiler = new QueryCompiler()
    doSelect()
    doInsertSelect()
  }

  private def doSelect()(implicit compiler: QueryCompiler) = {
    // compile the query
    val query = compiler.compile(
      """
        |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
        |FROM './companylist.csv'
        |WHERE Sector = 'Basic Industries'
        |LIMIT 10""".stripMargin)

    // execute the query
    val results = query.execute()

    // display the results as a table
    new Tabular().transform(results) foreach println
  }

  private def doInsertSelect()(implicit compiler: QueryCompiler) = {
    // compile the statement
    val statement = compiler.compile(
      """
        |INSERT INTO './test1.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
        |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
        |FROM './companylist.csv'
        |WHERE Sector = 'Basic Industries'""".stripMargin)

    // execute the query
    val results = statement.execute()

    // display the results as a table
    new Tabular().transform(results) foreach println
  }

}
