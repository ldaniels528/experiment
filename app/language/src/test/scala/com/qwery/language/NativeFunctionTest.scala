package com.qwery.language

import com.qwery.models.ColumnSpec
import com.qwery.models.expressions.Case.When
import com.qwery.models.expressions.{NativeFunctions => f, _}
import org.scalatest.funspec.AnyFunSpec

/**
  * Native Function Test Suite
  * @author lawrence.daniels@gmail.com
  */
class NativeFunctionTest extends AnyFunSpec {

  describe(classOf[NativeFunction].getSimpleName) {
    import com.qwery.models.expressions.implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("should parse: abs(value)") {
      verify("abs(value)", f.abs('value))
    }

    it("should parse: acos(value)") {
      verify("acos(value)", f.acos('value))
    }

    it("should parse: add_months(startDate, offset)") {
      verify("add_months(startDate, offset)", f.add_months('startDate, 'offset))
    }

    it("should parse: aggregate(value)") {
      verify("aggregate(value)", f.aggregate('value))
    }

    it("should parse: approx_count_distinct(value)") {
      verify("approx_count_distinct(value)", f.approx_count_distinct('value))
    }

    it("should parse: approx_percentile(value)") {
      verify("approx_percentile(value)", f.approx_percentile('value))
    }

    it("should parse: array_contains(array(1, 20, NULL, 3), 1)") {
      verify("array_contains(array(1, 20, NULL, 3), 1)", f.array_contains(f.array(1, 20, Null, 3), 1))
    }

    it("should parse: array_distinct(array(1, 20, NULL, 3))") {
      verify("array_distinct(array(1, 20, NULL, 3))", f.array_distinct(f.array(1, 20, Null, 3)))
    }

    it("should parse: array_Except(array(1, 20, NULL, 3), array(1, 3))") {
      verify("array_Except(array(1, 20, NULL, 3), array(1, 3))", f.array_except(f.array(1, 20, Null, 3), f.array(1, 3)))
    }

    it("should parse: array_max(array(1, 20, NULL, 3))") {
      verify("array_Max(array(1, 20, NULL, 3))", f.array_max(f.array(1, 20, Null, 3)))
    }

    it("should parse: array_min(array(1, 20, NULL, 3))") {
      verify("array_Min(array(1, 20, NULL, 3))", f.array_min(f.array(1, 20, Null, 3)))
    }

    it("should parse: ascii(value)") {
      verify("ascii(value)", f.ascii('value))
    }

    it("should parse: avg(value)") {
      verify("avg(value)", f.avg('value))
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
          When(FieldRef('Sector) === "Oil & Gas Production", "Oil-Gas"),
          When(FieldRef('Sector) === "Public Utilities", "Pub Utils")
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
          When(FieldRef('Sector) === "Oil & Gas Production", "Oil-Gas"),
          When(FieldRef('Sector) === "Public Utilities", "Pub Utils")
        )(otherwise = "Unknown": Expression))
    }

    it("should parse: cast(LastSale AS String)") {
      verify("cast(LastSale AS String)", Cast('LastSale, ColumnSpec("STRING")))
    }

    it("should parse: count(*)") {
      verify("count(*)", f.count('*))
    }

    it("should parse: count(LastSale)") {
      verify("count(LastSale)", f.count('LastSale))
    }

    it("should parse: from_unixtime(event_time, 'YYYY-MM-dd')") {
      verify("from_unixtime(event_time, 'YYYY-MM-dd')", f.from_unixtime('event_time, "YYYY-MM-dd"))
    }

    it("should parse: from_utc_timestamp(event_time, 'YYYY-MM-dd')") {
      verify("from_utc_timestamp(event_time, 'YYYY-MM-dd')", f.from_utc_timestamp('event_time, "YYYY-MM-dd"))
    }

    it("should parse: if(LastSale < 1, 'Penny Stock', 'Stock')") {
      verify("if(LastSale < 1, 'Penny Stock', 'Stock')", If(FieldRef('LastSale) < 1, "Penny Stock", "Stock"))
    }

    it("should parse: max(LastSale)") {
      verify("max(LastSale)", f.max('LastSale))
    }

    it("should parse: min(LastSale)") {
      verify("min(LastSale)", f.min('LastSale))
    }

    it("should parse: sum(LastSale)") {
      verify("sum(LastSale)", f.sum('LastSale))
    }

    it("should parse: trim('Hello World')") {
      verify("trim('Hello World')", f.trim("Hello World"))
    }

    it("should parse UDF: toDecimal(MarketCap)") {
      verify("toDecimal(MarketCap)", FunctionCall("toDecimal")('MarketCap))
    }

    it("should parse: year(startDate)") {
      verify("year(startDate)", f.year('startDate))
    }

  }

  def verify(expr: String, expect: Condition): Unit = {
    val actual = new ExpressionParser {}.parseCondition(TokenStream(expr))
    info(s"$expr => $actual")
    assert(actual.contains(expect), s"$expr : failed")
  }

  def verify(expr: String, expect: Expression): Unit = {
    val actual = new ExpressionParser {}.parseExpression(TokenStream(expr))
    info(s"$expr => $actual")
    assert(actual.contains(expect), s"$expr : failed")
  }

}
