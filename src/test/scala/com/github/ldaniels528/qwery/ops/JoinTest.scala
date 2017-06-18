package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.QweryCompiler
import com.github.ldaniels528.qwery.ops.sql._
import com.github.ldaniels528.qwery.sources.{DataResource, NamedResource}
import org.scalatest.FunSpec

/**
  * Join Tests
  * @author lawrence.daniels@gmail.com
  */
class JoinTest extends FunSpec {
  private val scope = RootScope()

  describe("InnerJoin") {

    it("should execute an INNER JOIN against two sources") {
      val count = InnerJoin(
        left = NamedResource(name = "A", DataResource("companylist.csv")),
        right = NamedResource(name = "B", DataResource("companylist2.csv")),
        condition = EQ(JoinColumnRef("B", "Symbol"), JoinColumnRef("A", "Symbol")))
        .execute(scope).count(_ => true)
      assert(count == 350)
    }

    it("should compile an INNER JOIN against two sources") {
      val ops = QweryCompiler(
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale
          |FROM "companylist.csv" AS A
          |INNER JOIN "companylist2.csv" AS B ON B.Symbol = A.Symbol
        """.stripMargin)
      
      assert(ops == Select(
        fields = List("Symbol", "Name", "Sector", "Industry", "LastSale").map(Field.apply),
        source = Option(NamedResource("A", DataResource("companylist.csv", hints = Option(Hints())))),
        joins = List(InnerJoin(
          left = NamedResource(name = "A", DataResource("companylist.csv", hints = Option(Hints()))),
          right = NamedResource(name = "B", DataResource("companylist2.csv", hints = Option(Hints()))),
          condition = EQ(JoinColumnRef("B", "Symbol"), JoinColumnRef("A", "Symbol"))
        ))
      ))
    }

  }

}
