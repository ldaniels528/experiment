package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.RootScope
import com.github.ldaniels528.tabular.Tabular

/**
  * Created by ldaniels on 6/9/17.
  */
object SQLAppTest extends App {
  val tabular = new Tabular()
  val scope = RootScope()

  val sql =
    """
      |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap, `Summary Quote`
      |FROM './companylist.csv'
      |WHERE Sector = 'Basic Industries'
      |LIMIT 5;
    """.stripMargin

  val op = QweryCompiler(sql)
  println(op)

  tabular.transform(op.execute(scope)) foreach println
}
