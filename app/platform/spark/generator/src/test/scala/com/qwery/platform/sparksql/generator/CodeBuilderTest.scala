package com.qwery.platform.sparksql.generator

import org.scalatest.FunSpec

/**
  * Code Builder Test Suite
  * @author lawrence.daniels@gmail.com
  */
class CodeBuilderTest extends FunSpec {

  describe(classOf[CodeBuilder].getSimpleName) {

    it("should make generating code easier") {
      val code = CodeBuilder(prepend = ".")
        .append("spark.read.csv('/some/path')")
        .append("toDF('1'.'2','3')")
        .append("select('1', '2', '3')")
        .build()

      info(code)
      assert(code ==
        """|spark.read.csv('/some/path')
           |  .toDF('1'.'2','3')
           |  .select('1', '2', '3')
           |""".stripMargin)
    }

    it("should generate code from s template") {
      val code = CodeBuilder.template(
        """|{{code1}}
           |  .{{code2}}
           |  .{{code3}}
           |""".stripMargin
      )(
        "code1" -> "spark.read.csv('/some/path')",
        "code2" -> "toDF('1'.'2','3')",
        "code3" -> "select('1', '2', '3')"
      )

      info(code)
      assert(code ==
        """|spark.read.csv('/some/path')
           |  .toDF('1'.'2','3')
           |  .select('1', '2', '3')
           |""".stripMargin)
    }

  }

}
