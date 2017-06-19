package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.QweryCompiler
import com.github.ldaniels528.qwery.QweryDecompiler._
import com.github.ldaniels528.qwery.ops.sql._
import com.github.ldaniels528.qwery.sources.{DataResource, NamedResource}
import com.github.ldaniels528.qwery.util.StringHelper._
import org.scalatest.FunSpec

/**
  * Join Tests
  * @author lawrence.daniels@gmail.com
  */
class JoinTest extends FunSpec {
  private val scope = Scope.root()

  describe("InnerJoin") {

    it("should compile an INNER JOIN query") {
      val opCode = QweryCompiler(
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale
          |FROM "companylist.csv" AS A
          |INNER JOIN "companylist2.csv" AS B ON B.Symbol = A.Symbol
        """.stripMargin)

      assert(opCode == Select(
        fields = List("Symbol", "Name", "Sector", "Industry", "LastSale").map(Field.apply),
        source = Option(NamedResource("A", DataResource("companylist.csv", hints = Option(Hints())))),
        joins = List(InnerJoin(
          leftAlias = Some("A"),
          right = NamedResource(name = "B", DataResource("companylist2.csv", hints = Option(Hints()))),
          condition = EQ(JoinField("B", "Symbol"), JoinField("A", "Symbol"))
        ))
      ))
    }

    it("should decompile an INNER JOIN object") {
      val opCode = Select(
        fields = List("Symbol", "Name", "Sector", "Industry", "LastSale").map(Field.apply),
        source = Option(NamedResource("A", DataResource("companylist.csv", hints = Option(Hints())))),
        joins = List(InnerJoin(
          leftAlias = Some("A"),
          right = NamedResource(name = "B", DataResource("companylist2.csv", hints = Option(Hints()))),
          condition = EQ(JoinField("B", "Symbol"), JoinField("A", "Symbol"))
        ))
      )

      assert(opCode.toSQL ==
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale FROM ('companylist.csv' AS A)
          |INNER JOIN 'companylist2.csv' AS B ON B.Symbol = A.Symbol """.stripMargin.toSingleLine)
    }

    it("should execute an INNER JOIN object") {
      val count =
        Select(
          fields = Seq(AllFields),
          source = Some(NamedResource(name = "A", DataResource("companylist.csv"))),
          joins = List(
            InnerJoin(
              leftAlias = Some("A"),
              right = NamedResource(name = "B", DataResource("companylist2.csv")),
              condition = EQ(JoinField("B", "Symbol"), JoinField("A", "Symbol")))
          )
        ).execute(scope).count(_ => true)
      assert(count == 350)
    }

  }

}
