package com.qwery.language

import com.qwery.models.expressions.NativeFunction
import org.scalatest.FunSpec

/**
  * Native Functions Test
  * @author lawrence.daniels@gmail.com
  */
class NativeFunctionsTest extends FunSpec {

  import NativeFunction._

  describe(classOf[NativeFunction].getSimpleName) {

    it("should generate short-cuts") {
      val results = nativeFunctions.toSeq.sortBy { case (name, _) => name } map {
        case (name, fx) if fx.minArgs == 1 && fx.maxArgs == 1 =>
          val realName = toRealName(name)
          s"""|/**
              |  * $realName - ${fx.description}
              |  * @example {{{ ${fx.usage} }}}
              |  */
              |object $realName {
              |   def apply(expr: Expression): FunctionCall = FunctionCall("$name")(expr)
              |}
              |""".stripMargin
        case (name, fx) =>
          val realName = toRealName(name)
          s"""|/**
              |  * $realName - ${fx.description}
              |  * @example {{{ ${fx.usage} }}}
              |  */
              |object $realName {
              |   def apply(expr: Expression*): FunctionCall = FunctionCall("$name")(expr:_*)
              |}
              |""".stripMargin
      }

      results.foreach(s => println(s))
    }

  }

  def toRealName(name: String):String = {
    name match {
      case s if s.contains("_") => s.split("[_]").map(_.capitalize).mkString("_")
      case s => s.capitalize
    }
  }

}
