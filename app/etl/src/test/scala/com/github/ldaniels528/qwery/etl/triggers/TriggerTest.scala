package com.github.ldaniels528.qwery.etl.triggers

import com.github.ldaniels528.qwery.QweryCompiler
import com.github.ldaniels528.qwery.ops.{Row, Scope}
import org.scalatest.FunSpec

/**
  * Trigger Tests
  * @author lawrence.daniels@gmail.com
  */
class TriggerTest extends FunSpec {
  private val scope = Scope.root()

  describe("Trigger") {

    it("should trigger the execution of files by prefix match") {
      val trigger = FileTrigger(
        name = "Company Lists",
        constraints = List(PrefixConstraint(prefix = "companylist")),
        executable = QweryCompiler(sql =
          """
            |INSERT INTO 'companylist.json' (Symbol, Name, Sector, Industry)
            |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
            |FROM 'companylist.csv' WITH CSV FORMAT
            |WHERE Industry = 'Oil/Gas Transmission'
          """.stripMargin))

      val results = trigger.execute(scope, path = "companylist.csv").toSeq
      assert(results == Seq(Row("ROWS_INSERTED" -> 4)))
    }

  }

}
