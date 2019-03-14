package com.qwery.platform.codegen.spark

import com.qwery.models._
import com.qwery.models.expressions._
import org.scalatest.FunSpec

/**
  * Spark Code Generator Test Suite
  * @author lawrence.daniels@gmail.com
  */
class SparkCodeGeneratorTest extends FunSpec {

  describe(classOf[SparkCodeGenerator].getSimpleName) {

    it("should generate a class file") {
      import com.qwery.util.OptionHelper.Implicits.Risky._
      import expressions.implicits._

      val generator = new SparkCodeGenerator(className = "Example1", packageName = "com.github.ldaniels528.qwery")
      generator.generate(MainProgram(name = "Oil_Companies", hiveSupport = true, streaming = true, code = SQL(
        Select(
          fields = List('Symbol, 'Name, 'Sector, 'Industry, 'SummaryQuote),
          from = Table("Customers"),
          where = Field('Industry) === $(name = "industry")
        )
      )))
    }

  }

}
