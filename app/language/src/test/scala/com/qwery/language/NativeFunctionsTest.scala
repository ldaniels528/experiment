package com.qwery.language

import java.io.{File, PrintWriter}

import com.qwery.models.expressions.{NativeFunction, NativeFunctions}
import com.qwery.util.OptionHelper._
import com.qwery.util.ResourceHelper._
import com.qwery.util.StringHelper._
import org.scalatest.FunSpec

import scala.io.Source

/**
  * Generates the Native Functions class file
  * @author lawrence.daniels@gmail.com
  * @see [[NativeFunctions]]
  */
class NativeFunctionsTest extends FunSpec {

  import NativeFunction._

  describe(classOf[NativeFunction].getSimpleName) {

    it("should generate short-cuts") {
      val members = nativeFunctions.toSeq.sortBy { case (name, _) => name } map {
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

      writeToDisk(NativeFunctions.getObjectFullName) {
        Source.fromString(members.mkString("\n")).getLines() map (line => "\t" + line) mkString "\n"
      }
    }

  }

  def toRealName(name: String): String = {
    name match {
      case s if s.contains("_") => s.split("[_]").map(_.capitalize).mkString("_")
      case s => s.capitalize
    }
  }

  def writeToDisk(fullyQualifiedClassName: String)(contents: => String): Unit = {
    val (packageName, className) = fullyQualifiedClassName.lastIndexOfOpt(".")
      .map(fullyQualifiedClassName.splitAt)
      .map { case (_pkg, _class) => _pkg -> _class.drop(1) }
      .orFail(s"Invalid class name - $fullyQualifiedClassName")

    val srcDir = new File("app.core.src.main.scala".replace('.', File.separatorChar))
    val classDir = new File(srcDir, packageName.replace('.', File.separatorChar))
    val classFile = new File(classDir, s"$className.scala")
    new PrintWriter(classFile) use (_.println(
      s"""|package $packageName
          |
          |/**
          |  * Native SQL Functions Model Facades
          |  * @author lawrence.daniels@gmail.com
          |  * @note This is a generated class.
          |  */
          |object $className {
          |
          | $contents
          |}
          |""".stripMargin
    ))
  }

}
