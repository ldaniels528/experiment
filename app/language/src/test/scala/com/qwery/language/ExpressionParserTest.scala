package com.qwery.language

import com.qwery.models.expressions._
import org.scalatest.FunSpec

/**
  * Expression Parser Test
  * @author lawrence.daniels@gmail.com
  */
class ExpressionParserTest extends FunSpec {

  describe(classOf[ExpressionParser].getSimpleName) {
    import com.qwery.models.expressions.Expression.Implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("""should parse conditional expression "100 < 1" (conditional expression)""") {
      verify("100 < 1", Literal(100) < 1)
    }

    it("""should parse "'Hello World' = 'Goodbye'" (conditional expression)""") {
      verify("'Hello World' = 'Goodbye'", Literal("Hello World") === "Goodbye")
    }

    it("""should parse "`Symbol` = 'AAPL'" (conditional expression)""") {
      verify("`Symbol` = 'AAPL'", Field("Symbol") === "AAPL")
    }

    it("""should parse "A.Symbol = 'AMD'" (conditional expression)""") {
      verify("A.Symbol = 'AMD'", Field("A.Symbol") === "AMD")
    }

    it("""should parse "Sum(A.LastSale) >= 5000" (conditional expression)""") {
      verify("Sum(A.LastSale) >= 5000", Sum(Field("A.LastSale")) >= 5000)
    }

    it("""should parse "Min(A.LastSale) <= 1" (conditional expression)""") {
      verify("Min(A.LastSale) <= 1", Min(Field("A.LastSale")) <= 1)
    }

    it("""should parse "y + (x * 2)" (expression)""") {
      verify("y + (x * 2)", Add(Field("y"), Field("x") * 2))
    }

    it("""should parse "y + (x / 2)" (expression)""") {
      verify("y + (x / 2)", Add(Field("y"), Field("x") / 2))
    }

    it("""should parse "(y - (x / 2)) AS 'calc'" (expression)""") {
      verify("(y - (x / 2))  AS 'calc'", Subtract(Field("y"), Field("x") / 2).as("calc"))
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

    it("should parse functions (If)") {
      verify("If(LastSale < 1, 'Penny Stock', 'Stock')", If(Field("LastSale") < 1, "Penny Stock", "Stock"))
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

    it("should parse user-defined function (UDF) calls: toDecimal(MarketCap)") {
      verify("toDecimal(MarketCap)", FunctionCall(name = "toDecimal")(Field("MarketCap")))
    }

    it("should parse local variables: \"$total\"") {
      verify("$total", LocalVariableRef("total"))
    }

    it("should parse row set variables: \"@results\"") {
      verify("@results", RowSetVariableRef("results"))
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
