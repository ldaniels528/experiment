package com.qwery.language

import com.qwery.models.ColumnTypes
import com.qwery.models.expressions.Case.When
import com.qwery.models.expressions.NativeFunctions._
import com.qwery.models.expressions._
import org.scalatest.FunSpec

/**
  * Expression Parser Test for SQL functions
  * @author lawrence.daniels@gmail.com
  */
class SQLFunctionTest extends FunSpec {

  describe(classOf[ExpressionParser].getSimpleName) {
    import com.qwery.models.expressions.implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("""should parse "Abs(value)"""") {
      verify("Abs(value)", Abs('value))
    }

    it("""should parse "Array_Max(Array(1, 20, NULL, 3))"""") {
      verify("Array_Max(Array(1, 20, NULL, 3))" , Array_Max(Array(1, 20, Null, 3)))
    }

    it("""should parse "Array_Min(ARRAY(1, 20, NULL, 3))"""") {
      verify("Array_Min(Array(1, 20, NULL, 3))" , Array_Min(Array(1, 20, Null, 3)))
    }

    it("""should parse "Ascii(value)"""") {
      verify("Ascii(value)", Ascii('value))
    }

    it("""should parse "Avg(value)"""") {
      verify("Avg(value)", Avg('value))
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
          When(Field('Sector) === "Oil & Gas Production", "Oil-Gas"),
          When(Field('Sector) === "Public Utilities", "Pub Utils")
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
          When(Field('Sector) === "Oil & Gas Production", "Oil-Gas"),
          When(Field('Sector) === "Public Utilities", "Pub Utils")
        )(otherwise = "Unknown": Expression))
    }

    it("""should parse "Cast(LastSale AS String)" """) {
      verify("Cast(LastSale AS String)", Cast('LastSale, ColumnTypes.STRING))
    }

    it("""should parse "Count(LastSale)" """) {
      verify("Count(LastSale)", Count('LastSale))
    }

    it("""should parse "Count(*)" """) {
      verify("Count(*)", Count('*))
    }

    it("should parse functions (If)") {
      verify("If(LastSale < 1, 'Penny Stock', 'Stock')", If(Field('LastSale) < 1, "Penny Stock", "Stock"))
    }

    /* TODO fix me
    it("""should parse "from_unixtime(cast(trim(event_time) as bigint) - 10800)"""") {
      verify("(from_unixtime(cast(trim(event_time) as bigint) - 10800))", From_UnixTime(Cast(Trim(Field("event_time")), ColumnTypes.BIGINT) - 10800))
    }
    */

    it("""should parse "Min(A.LastSale) <= 1" (conditional expression)""") {
      verify("Min(A.LastSale) <= 1", Min(Field("A.LastSale")) <= 1)
    }

    it("""should parse "Sum(A.LastSale) >= 5000"""") {
      verify("Sum(A.LastSale) >= 5000", Sum(Field("A.LastSale")) >= 5000)
    }

    it("should parse user-defined function (UDF) calls: toDecimal(MarketCap)") {
      verify("toDecimal(MarketCap)", FunctionCall("toDecimal")('MarketCap))
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
