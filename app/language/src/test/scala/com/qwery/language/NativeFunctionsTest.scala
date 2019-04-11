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
        case (name, fx) if fx.minArgs == 0 && fx.maxArgs == 0 =>
          val realName = toRealName(name)
          s"""|/**
              |  * ${fx.description}.
              |  * @example {{{ ${fx.usage} }}}
              |  */
              |object $realName {
              |   def apply(): FunctionCall = FunctionCall("$name")()
              |}
              |""".stripMargin
        case (name, fx) if fx.minArgs == 1 && fx.maxArgs == 1 =>
          val realName = toRealName(name)
          s"""|/**
              |  * ${fx.description}.
              |  * @example {{{ ${fx.usage} }}}
              |  */
              |object $realName {
              |   def apply(expr: Expression): FunctionCall = FunctionCall("$name")(expr)
              |}
              |""".stripMargin
        case (name, fx) if fx.minArgs == 2 && fx.maxArgs == 2 =>
          val realName = toRealName(name)
          s"""|/**
              |  * ${fx.description}.
              |  * @example {{{ ${fx.usage} }}}
              |  */
              |object $realName {
              |   def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("$name")(expr1, expr2)
              |}
              |""".stripMargin
        case (name, fx) if fx.minArgs == 3 && fx.maxArgs == 3 =>
          val realName = toRealName(name)
          s"""|/**
              |  * ${fx.description}.
              |  * @example {{{ ${fx.usage} }}}
              |  */
              |object $realName {
              |   def apply(expr1: Expression, expr2: Expression, expr3: Expression): FunctionCall = FunctionCall("$name")(expr1, expr2, expr3)
              |}
              |""".stripMargin
        case (name, fx) =>
          val realName = toRealName(name)
          s"""|/**
              |  * ${fx.description}.
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
