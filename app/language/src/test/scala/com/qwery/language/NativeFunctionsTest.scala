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
        case (name, fx) if fx.minArgs == 0 && fx.maxArgs == 0 => zero(name, fx)
        case (name, fx) if fx.minArgs == 1 && fx.maxArgs == 1 => one(name, fx)
        case (name, fx) if fx.minArgs == 2 && fx.maxArgs == 2 => two(name, fx)
        case (name, fx) if fx.minArgs == 3 && fx.maxArgs == 3 => three(name, fx)
        case (name, fx) => variable(name, fx)
      }

      writeToDisk(NativeFunctions.getObjectFullName) {
        Source.fromString(members.mkString("\n")).getLines() map (line => " " * 2 + line) mkString "\n"
      }
    }

    def zero(name: String, fx: NativeFunction): String = {
      s"""|/**
          |  * ${fx.description}.
          |  * @example {{{ ${fx.usage} }}}
          |  */
          |def $name(): Expression = FunctionCall("$name")()
          |""".stripMargin
    }

    def one(name: String, fx: NativeFunction): String = {
      s"""|/**
          |  * ${fx.description}.
          |  * @example {{{ ${fx.usage} }}}
          |  */
          |def ${fixName(name)}(expr: Expression): Expression = FunctionCall("$name")(expr)
          |""".stripMargin
    }

    def two(name: String, fx: NativeFunction): String = {
      s"""|/**
          |  * ${fx.description}.
          |  * @example {{{ ${fx.usage} }}}
          |  */
          |def ${fixName(name)}(expr1: Expression, expr2: Expression): Expression = FunctionCall("$name")(expr1, expr2)
          |""".stripMargin
    }

    def three(name: String, fx: NativeFunction): String = {
      s"""|/**
          |  * ${fx.description}.
          |  * @example {{{ ${fx.usage} }}}
          |  */
          |def ${fixName(name)}(expr1: Expression, expr2: Expression, expr3: Expression): Expression = FunctionCall("$name")(expr1, expr2, expr3)
          |""".stripMargin
    }

    def variable(name: String, fx: NativeFunction): String = {
      s"""|/**
          |  * ${fx.description}.
          |  * @example {{{ ${fx.usage} }}}
          |  */
          |def ${fixName(name)}(expr: Expression*): Expression = FunctionCall("$name")(expr:_*)
          |""".stripMargin
    }

    def fixName(name: String): String = name match {
      case "if" => "`if`"
      case x => x
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
          |trait $className {
          |
          |$contents
          |
          |}
          |
          |/**
          |  * Native SQL Functions Singleton
          |  * @author lawrence.daniels@gmail.com
          |  */
          |object $className extends $className
          |""".stripMargin
    ))
  }

}
