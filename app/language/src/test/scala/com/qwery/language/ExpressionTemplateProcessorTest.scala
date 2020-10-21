package com.qwery.language

import com.qwery.models._
import com.qwery.models.expressions.implicits._
import com.qwery.models.expressions.{Field, Literal}
import org.scalatest.funspec.AnyFunSpec

/**
  * Expression Template Processor Test Suite
  * @author lawrence.daniels@gmail.com
  */
class ExpressionTemplateProcessorTest extends AnyFunSpec {
  private implicit val parser: ExpressionParser = new ExpressionParser {}
  private val processor = new ExpressionTemplateProcessor {}

  describe(classOf[ExpressionTemplateProcessor].getSimpleName) {

    it("should process CAST(1234 as String)") {
      val ts = TokenStream("CAST(1234 as String)")
      val results = processor.process("CAST ( %e:value AS %t:type )", ts)
      assert(results.expressions.get("value").contains(Literal(1234)))
      assert(results.types.get("type").contains(ColumnSpec("STRING")))
    }

    it("should process IF(LastSale < 1, 'Penny Stock', 'Stock')") {
      val ts = TokenStream("IF(LastSale < 1, 'Penny Stock', 'Stock')")
      val results = processor.process("IF ( %c:condition , %e:trueValue , %e:falseValue )", ts)
      assert(results.conditions.get("condition").contains(Field('LastSale) < 1))
      assert(results.expressions.get("trueValue").contains(Literal("Penny Stock")))
      assert(results.expressions.get("falseValue").contains(Literal("Stock")))
    }

    it("should process PRINT(@deptCode)") {
      val ts = TokenStream("PRINT(@deptCode)")
      val results = processor.process("PRINT ( %v:variable )", ts)
      assert(results.variables.get("variable").contains(@@("deptCode")))
    }

    it("should process SUBSTRING('Hello World', 5, 1)") {
      val ts = TokenStream("SUBSTRING('Hello World', 5, 1)")
      val results = processor.process("SUBSTRING ( %E:values )", ts)
      assert(results.expressionLists.get("values").contains(List("Hello World", 5, 1).map(Literal.apply)))
    }

    it("should process SUM(deptCode)") {
      val ts = TokenStream("SUM(deptCode)")
      val results = processor.process("SUM ( %f:field )", ts)
      assert(results.fields.get("field").contains(Field("deptCode")))
    }

  }

}
