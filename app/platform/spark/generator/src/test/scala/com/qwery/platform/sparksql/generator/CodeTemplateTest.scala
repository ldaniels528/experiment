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

      implicit val args: ApplicationArgs = ApplicationArgs()
      implicit val ctx: CompileContext = CompileContext(invokable)

      val template = CodeTemplate.fromString(
        """|/**
           |  * Coyote Trap
           |  * @author willy.coyote@gmail.com
           |  */
           |class {{className}} {
           |  {{flow}}
           |}
           |""".stripMargin
      )
      val actual = template.generate(
        className = "CoyoteTrap",
        packageName = "com.acme.tools",
        outputPath = "./temp",
        invokable = invokable
      )
      val expected =
        """|/**
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
