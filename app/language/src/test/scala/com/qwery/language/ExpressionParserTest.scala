package com.qwery.language

import com.qwery.models.expressions._
import com.qwery.language.ExpressionParser._
import org.scalatest.FunSpec

/**
  * Expression Parser Test
  * @author lawrence.daniels@gmail.com
  */
class ExpressionParserTest extends FunSpec {

  describe(classOf[ExpressionParser].getSimpleName) {
    import com.qwery.models.expressions.Expression.Implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("""should identify "100" as a constant""") {
      assert(TokenStream("100 = 1").isConstant)
    }

    it("""should identify "'Hello World'" as a constant""") {
      assert(TokenStream("'Hello World' = 1").isConstant)
    }

    it("""should identify "`Symbol`" as a field""") {
      assert(TokenStream("`Symbol` = 100").isField)
    }

    it("""should identify "Symbol" as a field""") {
      assert(TokenStream("Symbol = 100").isField)
    }

    it("""should identify "A.Symbol" as a JOIN column""") {
      assert(TokenStream("A.Symbol = 100").isJoinColumn)
    }

    it("""should identify "Sum(A.Symbol)" as a function""") {
      assert(TokenStream("Sum(A.Symbol) = 100").isFunction)
    }

    it("""should NOT identify "ABC + (1 * x)" as a function""") {
      assert(!TokenStream("ABC + (1 * x)").isFunction)
    }

    it("""should parse "LastSale = 100" (equal)""") {
      verify("LastSale = 100", Field("LastSale") === 100)
    }

    it("""should parse "LastSale != 101" (not equal)""") {
      verify("LastSale != 101", Field("LastSale") !== 101)
    }

    it("""should parse "LastSale <> 102" (not equal)""") {
      verify("LastSale <> 102", Field("LastSale") !== 102)
    }

    it("""should parse "NOT LastSale = 103" (not equal)""") {
      verify("NOT LastSale = 103", NOT(Field("LastSale") === 103))
    }

    it("""should parse "LastSale > 104" (greater)""") {
      verify("LastSale > 104", Field("LastSale") > 104)
    }

    it("""should parse "LastSale >= 105" (greater or equal)""") {
      verify("LastSale >= 105", Field("LastSale") >= 105)
    }

    it("""should parse "LastSale < 106" (lesser)""") {
      verify("LastSale < 106", Field("LastSale") < 106)
    }

    it("""should parse "LastSale <= 107" (lesser or equal)""") {
      verify("LastSale <= 107", Field("LastSale") <= 107)
    }

    it("""should parse "Sector IS NULL" (IS NULL)""") {
      verify("Sector IS NULL", IsNull(Field("Sector")))
    }

    it("""should parse "Sector IS NOT NULL" (IS NOT NULL)""") {
      verify("Sector IS NOT NULL", IsNotNull(Field("Sector")))
    }

    it("""should parse expressions containing 'AND'""") {
      verify("Sector = 'Basic Industries' AND Industry = 'Gas & Oil'",
        Field("Sector") === "Basic Industries" && Field("Industry") === "Gas & Oil")
    }

    it("""should parse expressions containing 'OR'""") {
      verify("Sector = 'Basic Industries' OR Industry = 'Gas & Oil'",
        Field("Sector") === "Basic Industries" || Field("Industry") === "Gas & Oil")
    }

    it("""should parse expressions containing 'AND' and 'OR'""") {
      verify("Sector = 'Basic Industries' AND Industry LIKE '%Gas%' OR Industry LIKE '%Oil%'",
        Field("Sector") === "Basic Industries" && LIKE(Field("Industry"), "%Gas%") || LIKE(Field("Industry"), "%Oil%"))
    }

    it("""should parse "(x + 3) * 2" (quantities)""") {
      verify("(x + 3) * 2", Multiply(Add(Field("x"), 3), 2))
    }

    it("""should parse "Avg(LastSale)" (Avg)""") {
      verify("Avg(LastSale)", Avg(Field("LastSale")))
    }

    it("should parse functions (Case - Type 1)") {
      verify(
        """|CASE Sector
           |  WHEN 'Oil & Gas Production' THEN 'Oil-Gas'
           |  WHEN 'Public Utilities' THEN 'Pub Utils'
           |  ELSE 'Unknown'
           |END
           |""".stripMargin,
        Case(
          When(Field("Sector") === "Oil & Gas Production", "Oil-Gas"),
          When(Field("Sector") === "Public Utilities", "Pub Utils")
        )(otherwise = "Unknown": Expression))
    }

    it("should parse functions (Case - Type 2)") {
      verify(
        """|CASE
           |  WHEN Sector = 'Oil & Gas Production' THEN 'Oil-Gas'
           |  WHEN Sector = 'Public Utilities' THEN 'Pub Utils'
           |  ELSE 'Unknown'
           |END
           |""".stripMargin,
        Case(
          When(Field("Sector") === "Oil & Gas Production", "Oil-Gas"),
          When(Field("Sector") === "Public Utilities", "Pub Utils")
        )(otherwise = "Unknown": Expression))
    }

    it("""should parse "Cast(LastSale AS 'String')" """) {
      verify("Cast(LastSale AS 'String')", Cast(Field("LastSale"), "String"))
    }

    it("""should parse "Count(LastSale)" """) {
      verify("Count(LastSale)", Count(Field("LastSale")))
    }

    it("""should parse "Count(*)" """) {
      verify("Count(*)", Count(AllFields))
    }

    it("should parse functions (Min)") {
      verify("Min(LastSale)", Min(Field("LastSale")))
    }

    it("should parse functions (Max)") {
      verify("Max(LastSale)", Max(Field("LastSale")))
    }

    it("should parse functions (PadLeft)") {
      verify("PadLeft(Symbol, 5)", PadLeft(Field("Symbol"), 5))
    }

    it("should parse functions (PadRight)") {
      verify("PadRight(Symbol, 5)", PadRight(Field("Symbol"), 5))
    }

    it("should parse functions (StdDev)") {
      verify("StdDev(LastSale)", StdDev(Field("LastSale")))
    }

    it("should parse functions (Substring)") {
      verify("Substring(Sector, 1, 5)", Substring(Field("Sector"), 1, 5))
    }

    it("should parse functions (Sum)") {
      verify("Sum(LastSale)", Sum(Field("LastSale")))
    }

  }

  private def verify(expr: String, expect: Condition): Unit = {
    val actual = new ExpressionParser {}.parseCondition(TokenStream(expr))
    info(s"$expr => $actual")
    assert(actual.contains(expect), s"$expr : failed")
  }

  private def verify(expr: String, expect: Expression): Unit = {
    val actual = new ExpressionParser {}.parseExpression(TokenStream(expr))
    info(s"$expr => $actual")
    assert(actual.contains(expect), s"$expr : failed")
  }

}
