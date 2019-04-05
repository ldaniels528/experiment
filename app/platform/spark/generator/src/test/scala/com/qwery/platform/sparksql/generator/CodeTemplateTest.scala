package com.qwery.platform.sparksql.generator

import com.qwery.language.SQLLanguageParser
import org.scalatest.FunSpec

/**
  * Code Template Test Suite
  * @author lawrence.daniels@gmail.com
  */
class CodeTemplateTest extends FunSpec {

  describe(classOf[CodeTemplate].getSimpleName) {

    it("should properly replace template value") {
      val invokable = SQLLanguageParser.parse(
        """|BEGIN
           |  PRINT 'Hello World'
           |END
           |""".stripMargin)

      implicit val args: ApplicationSettings = ApplicationSettings(Array(
        "--app-name", "Coyote Trap",
        "--input-path", "./scripts/daily-report.sql",
        "--output-path", "./temp",
        "--class-name", "com.acme.tools.CoyoteTrap"
      ))
      implicit val ctx: CompileContext = CompileContext(invokable)

      val template = CodeTemplate.fromString(
        """|package {{ packageName }}
           |
           |/**
           |  * {{ appName }}
           |  * @author willy.coyote@gmail.com
           |  */
           |class {{ className }} {
           |  {{ flow }}
           |}
           |""".stripMargin
      )
      val actual = template.generate(invokable)
      val expected =
        """|package com.acme.tools
           |
           |/**
           |  * Coyote Trap
           |  * @author willy.coyote@gmail.com
           |  */
           |class CoyoteTrap {
           |  println("Hello World")
           |}
           |""".stripMargin

      info(actual)
      assert(actual == expected)
    }

  }

}
