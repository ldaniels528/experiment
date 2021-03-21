package com.qwery.language

import com.qwery.models._
import com.qwery.models.expressions.Case.When
import com.qwery.models.expressions.{NativeFunctions => f, _}
import org.scalatest.funspec.AnyFunSpec

/**
  * Expression Parser Test
  * @author lawrence.daniels@gmail.com
  */
class ExpressionParserTest extends AnyFunSpec {

  describe(classOf[ExpressionParser].getSimpleName) {
    import com.qwery.models.expressions.implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("""it should parse "CASE WHEN field NOT LIKE '%.%' THEN 'Yes' ELSE 'No' END" """) {
      verify("CASE WHEN field NOT LIKE '%.%' THEN 'Yes' ELSE 'No' END",
        Case(When(NOT(LIKE('field, "%.%")), "Yes"))(otherwise = Some("No": Literal)))
    }

    it("""should parse "DISTINCT(PROPERTY_VAL)" """) {
      verify("DISTINCT(PROPERTY_VAL)", Distinct('PROPERTY_VAL))
    }

    it("""should parse "DISTINCT A.Symbol, A.Exchange" """) {
      verify("DISTINCT A.Symbol, A.Exchange", Distinct(JoinFieldRef("Symbol", tableAlias = "A"), JoinFieldRef("Exchange", tableAlias = "A")))
    }

    it("""should parse "INTERVAL 7 DAYS" (expression)""") {
      verify("INTERVAL 7 DAYS", Interval(7, IntervalTypes.DAY))
    }

    it("""should parse "A.Symbol" (expression)""") {
      verify("A.Symbol", JoinFieldRef("Symbol", tableAlias = "A"))
    }

    it("""should parse "A.Symbol IN ('AAPL', 'AMZN', 'AMD')" """) {
      verify("A.Symbol IN ('AAPL', 'AMZN', 'AMD')", IN(JoinFieldRef("Symbol", tableAlias = "A"))("AAPL", "AMZN", "AMD"))
    }

    it("""should parse conditional expression "100 < 1" (conditional expression)""") {
      verify("100 < 1", Literal(100) < 1)
    }

    it("""should parse "'Hello World' = 'Goodbye'" (conditional expression)""") {
      verify("'Hello World' = 'Goodbye'", Literal("Hello World") === "Goodbye")
    }

    it("""should parse "`Symbol` = 'AAPL'" (conditional expression)""") {
      verify("`Symbol` = 'AAPL'", FieldRef('Symbol) === "AAPL")
    }

    it("""should parse "A.Symbol = 'AMD'" (conditional expression)""") {
      verify("A.Symbol = 'AMD'", FieldRef("A.Symbol") === "AMD")
    }

    it("""should parse "y + (x * 2)" (expression)""") {
      verify("y + (x * 2)", FieldRef('y) + (FieldRef('x) * 2))
    }

    it("""should parse "y + (x / 2)" (expression)""") {
      verify("y + (x / 2)", FieldRef('y) + (FieldRef('x) / 2))
    }

    it("""should parse "(y - (x / 2)) AS 'calc'" (expression)""") {
      verify("(y - (x / 2))  AS 'calc'", FieldRef('y) - (FieldRef('x) / 2).as("calc"))
    }

    it("""should parse "LastSale = 100" (equal)""") {
      verify("LastSale = 100", FieldRef('LastSale) === 100)
    }

    it("""should parse "LastSale != 101" (not equal)""") {
      verify("LastSale != 101", FieldRef('LastSale) !== 101)
    }

    it("""should parse "LastSale <> 102" (not equal)""") {
      verify("LastSale <> 102", FieldRef('LastSale) !== 102)
    }

    it("""should parse "NOT LastSale = 103" (not equal)""") {
      verify("NOT LastSale = 103", NOT(FieldRef('LastSale) === 103))
    }

    it("""should parse "LastSale > 104" (greater)""") {
      verify("LastSale > 104", FieldRef('LastSale) > 104)
    }

    it("""should parse "LastSale >= 105" (greater or equal)""") {
      verify("LastSale >= 105", FieldRef('LastSale) >= 105)
    }

    it("""should parse "LastSale < 106" (lesser)""") {
      verify("LastSale < 106", FieldRef('LastSale) < 106)
    }

    it("""should parse "LastSale <= 107" (lesser or equal)""") {
      verify("LastSale <= 107", FieldRef('LastSale) <= 107)
    }

    it("""should parse "Sector IS NULL" (IS NULL)""") {
      verify("Sector IS NULL", IsNull('Sector))
    }

    it("""should parse "Sector IS NOT NULL" (IS NOT NULL)""") {
      verify("Sector IS NOT NULL", IsNotNull('Sector))
    }

    it("""should parse expressions containing 'AND'""") {
      verify("Sector = 'Basic Industries' AND Industry = 'Gas & Oil'",
        FieldRef('Sector) === "Basic Industries" && FieldRef('Industry) === "Gas & Oil")
    }

    it("""should parse expressions containing 'OR'""") {
      verify("Sector = 'Basic Industries' OR Industry = 'Gas & Oil'",
        FieldRef('Sector) === "Basic Industries" || FieldRef('Industry) === "Gas & Oil")
    }

    it("""should parse expressions containing 'AND' and 'OR'""") {
      verify("Sector = 'Basic Industries' AND Industry LIKE '%Gas%' OR Industry LIKE '%Oil%'",
        FieldRef('Sector) === "Basic Industries" && LIKE('Industry, "%Gas%") || LIKE('Industry, "%Oil%"))
    }

    it("""should parse "(x + 3) * 2" (quantities)""") {
      verify("(x + 3) * 2", (FieldRef('x) + 3) * 2)
    }

    it("should parse functions (Min)") {
      verify("Min(LastSale)", f.min('LastSale))
    }

    it("should parse functions (Max)") {
      verify("Max(LastSale)", f.max('LastSale))
    }

    it("should parse functions (lpad)") {
      verify("lpad(Symbol, 5, ' ')", f.lpad('Symbol, 5, " "))
    }

    it("should parse functions (printf)") {
      verify("""printf("Hello World %d %s", 100, "days")""", f.printf("Hello World %d %s", 100, "days"))
    }

    it("should parse functions (Rpad)") {
      verify("RPad(Symbol, 5, ' ')", f.rpad('Symbol, 5, " "))
    }

    it("should parse functions (Stddev)") {
      verify("StdDev(LastSale)", f.stddev('LastSale))
    }

    it("should parse functions (Substring)") {
      verify("Substring(Sector, 1, 5)", f.substring('Sector, 1, 5))
    }

    it("should parse functions (Sum)") {
      verify("Sum(LastSale)", f.sum('LastSale))
    }

    it("should parse local variables: \"$total\"") {
      verify("$total", $$("total"))
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
