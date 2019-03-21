package com.qwery.platform.codegen.spark

import com.qwery.models.expressions.{Field, IsNotNull, IsNull}
import com.qwery.models.expressions.SQLFunction._
import com.qwery.platform.codegen.spark.SparkCodeCompiler._
import org.scalatest.FunSpec

/**
  * Spark Code Compiler Test
  * @author lawrence.daniels@gmail.com
  */
class SparkCodeCompilerTest extends FunSpec {

  import com.qwery.models.expressions.implicits._

  describe(SparkCodeCompiler.getClass.getSimpleName.replaceAllLiterally("$", "")) {

    it("should compile conditions: age === 50") {
      assert((Field('age) === 50).compile == """$"age" === lit(50)""")
    }

    it("should compile conditions: age !== 50") {
      assert((Field('age) !== 50).compile == """$"age" =!= lit(50)""")
    }

    it("should compile conditions: age < 50") {
      assert((Field('age) < 50).compile == """$"age" < lit(50)""")
    }

    it("should compile conditions: age <= 50") {
      assert((Field('age) <= 50).compile == """$"age" <= lit(50)""")
    }

    it("should compile conditions: age > 50") {
      assert((Field('age) > 50).compile == """$"age" > lit(50)""")
    }

    it("should compile conditions: age >= 50") {
      assert((Field('age) >= 50).compile == """$"age" >= lit(50)""")
    }

    it("should compile conditions: content is not null") {
      assert(IsNotNull('content).compile == """$"content".isNotNull""")
    }

    it("should compile conditions: content is null") {
      assert(IsNull('content).compile == """$"content".isNull""")
    }

    it("should compile conditions: max(age) < 50") {
      assert((Max('age) < 50).compile == """max($"age") < lit(50)""")
    }

    it("should compile conditions: min(age) > 10") {
      assert((Min('age) > 10).compile == """min($"age") > lit(10)""")
    }

    it("should compile conditions: substr(title, 1, 4) === \"Hello\"") {
      assert((Substring('title, 1, 4) === "Hello").compile == """substring($"title", 1, 4) === lit("Hello")""")
    }

    it("should compile conditions: sum(cost) < 5000") {
      assert((Sum('cost) < 5000).compile == """sum($"cost") < lit(5000)""")
    }

  }

}
