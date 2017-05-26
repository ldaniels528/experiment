package com.github.ldaniels528.qwery.etl.triggers

import com.github.ldaniels528.qwery.ops.RootScope
import org.scalatest.FunSpec

/**
  * Trigger Tests
  * @author lawrence.daniels@gmail.com
  */
class TriggerTest extends FunSpec {
  private val scope = RootScope()

  describe("Trigger") {

    it("should trigger the execution of files by prefix match") {
      val trigger = FileTrigger(
        name = "Company Lists",
        constraints = List(PrefixConstraint(prefix = "companylist")),
        script =
          """
            |INSERT INTO 'companylist.json' WITH JSON FORMAT (Symbol, Name, Sector, Industry)
            |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
            |FROM 'companylist.csv' WITH CSV FORMAT
            |WHERE Industry = 'Oil/Gas Transmission'
          """.stripMargin)

      val results = trigger.execute(scope, file = "companylist.csv").toSeq
      assert(results == Seq(Seq("ROWS_INSERTED" -> 4)))
    }

  }

}
