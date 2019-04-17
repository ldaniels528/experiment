package com.qwery.language

import com.qwery.models.ColumnTypes
import com.qwery.models.expressions.Case.When
import com.qwery.models.expressions.NativeFunctions._
import com.qwery.models.expressions._
import org.scalatest.FunSpec

/**
  * Native Function Test Suite
  * @author lawrence.daniels@gmail.com
  */
class NativeFunctionTest extends FunSpec {

  describe(classOf[NativeFunction].getSimpleName) {
    import com.qwery.models.expressions.implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("should parse: Abs(value)") {
      verify("Abs(value)", Abs('value))
    }

    it("should parse: Acos(value)") {
      verify("Acos(value)", Acos('value))
    }

    it("should parse: Add_Months(startDate, offset)") {
      verify("Add_Months(startDate, offset)", Add_Months('startDate, 'offset))
    }

    it("should parse: Aggregate(value)") {
      verify("Aggregate(value)", Aggregate('value))
    }

    it("should parse: Approx_Count_Distinct(value)") {
      verify("Approx_Count_Distinct(value)", Approx_Count_Distinct('value))
    }

    it("should parse: Approx_Percentile(value)") {
      verify("Approx_Percentile(value)", Approx_Percentile('value))
    }

    it("should parse: Array_Contains(Array(1, 20, NULL, 3), 1)") {
      verify("Array_Contains(Array(1, 20, NULL, 3), 1)" , Array_Contains(Array(1, 20, Null, 3), 1))
    }

    it("should parse: Array_Distinct(Array(1, 20, NULL, 3))") {
      verify("Array_Distinct(Array(1, 20, NULL, 3))" , Array_Distinct(Array(1, 20, Null, 3)))
    }

    it("should parse: Array_Except(Array(1, 20, NULL, 3), Array(1, 3))") {
      verify("Array_Except(Array(1, 20, NULL, 3), Array(1, 3))" , Array_Except(Array(1, 20, Null, 3), Array(1, 3)))
    }

    it("should parse: Array_Max(Array(1, 20, NULL, 3))") {
      verify("Array_Max(Array(1, 20, NULL, 3))" , Array_Max(Array(1, 20, Null, 3)))
    }

    it("should parse: Array_Min(Array(1, 20, NULL, 3))") {
      verify("Array_Min(Array(1, 20, NULL, 3))" , Array_Min(Array(1, 20, Null, 3)))
    }

    it("should parse: Ascii(value)") {
      verify("Ascii(value)", Ascii('value))
    }

    it("should parse: Avg(value)") {
      verify("Avg(value)", Avg('value))
    }

    it("should parse: CASE field ...") {
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

    it("should parse: CASE WHEN field ...") {
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

    it("should parse: Cast(LastSale AS String)") {
      verify("Cast(LastSale AS String)", Cast('LastSale, ColumnTypes.STRING))
    }

    it("should parse: Count(*)") {
      verify("Count(*)", Count('*))
    }

    it("should parse: Count(LastSale)") {
      verify("Count(LastSale)", Count('LastSale))
    }

    it("should parse: From_Unixtime(event_time, 'YYYY-MM-dd')") {
      verify("From_Unixtime(event_time, 'YYYY-MM-dd')", From_Unixtime('event_time, "YYYY-MM-dd"))
    }

    it("should parse: From_Utc_Timestamp(event_time, 'YYYY-MM-dd')") {
      verify("From_Utc_Timestamp(event_time, 'YYYY-MM-dd')", From_Utc_Timestamp('event_time, "YYYY-MM-dd"))
    }

    it("should parse: If(LastSale < 1, 'Penny Stock', 'Stock')") {
      verify("If(LastSale < 1, 'Penny Stock', 'Stock')", If(Field('LastSale) < 1, "Penny Stock", "Stock"))
    }

    it("should parse: Max(LastSale)") {
      verify("Max(LastSale)", Max('LastSale))
    }

    it("should parse: Min(LastSale)") {
      verify("Min(LastSale)", Min('LastSale))
    }

    it("should parse: Sum(LastSale)") {
      verify("Sum(LastSale)", Sum('LastSale))
    }

    it("should parse: Trim('Hello World')") {
      verify("Trim('Hello World')", Trim("Hello World"))
    }

    it("should parse UDF: toDecimal(MarketCap)") {
      verify("toDecimal(MarketCap)", FunctionCall("toDecimal")('MarketCap))
    }

    it("should parse: Year(startDate)") {
      verify("Year(startDate)", Year('startDate))
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
