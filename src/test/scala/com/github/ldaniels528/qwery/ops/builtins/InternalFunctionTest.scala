package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.Implicits._
import com.github.ldaniels528.qwery.ops.RootScope
import org.scalatest.FunSpec

/**
  * Internal Function Tests
  * @author lawrence.daniels@gmail.com
  */
class InternalFunctionTest extends FunSpec {
  private val scope = RootScope()

  describe("Left") {
    it("should extract a substring for N characters from left to right") {
      val result = Left("Hello World", 5).evaluate(scope)
      assert(result.contains("Hello"))
    }
  }

  describe("PadLeft") {
    it("should right-justify the a string") {
      val result = PadLeft("Hello", 10).evaluate(scope)
      assert(result.contains("     Hello"))
    }
  }

  describe("PadRight") {
    it("should left-justify the a string") {
      val result = PadRight("Hello", 10).evaluate(scope)
      assert(result.contains("Hello     "))
    }
  }

  describe("Right") {
    it("should extract a substring for N characters from right to left") {
      val result = Right("Hello World", 5).evaluate(scope)
      assert(result.contains("World"))
    }
  }

}
