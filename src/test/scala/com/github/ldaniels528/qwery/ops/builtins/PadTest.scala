package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.Implicits._
import com.github.ldaniels528.qwery.ops.{Expression, RootScope}
import org.scalatest.FunSpec

/**
  * Pad Function Tests
  * @author lawrence.daniels@gmail.com
  */
class PadTest extends FunSpec {
  private val scope = RootScope()

  describe("PadLeft") {

    it("should right-justify the a string") {
      val string: Expression = "Hello"
      val width: Expression = 10
      val result = PadLeft(string, width).evaluate(scope)
      assert(result.contains("     Hello"))
    }
  }

  describe("PadRight") {

    it("should left-justify the a string") {
      val string: Expression = "Hello"
      val width: Expression = 10
      val result = PadRight(string, width).evaluate(scope)
      assert(result.contains("Hello     "))
    }
  }

}
