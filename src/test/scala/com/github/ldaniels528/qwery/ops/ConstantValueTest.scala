package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.ops.Implicits._
import org.scalatest.FunSpec

/**
  * Constant Value Tests
  * @author lawrence.daniels@gmail.com
  */
class ConstantValueTest extends FunSpec {
  private val scope = RootScope()

  describe("ConstantValue") {
    it("should support determine when Boolean values are equivalent") {
      val value0 = ConstantValue(true)
      val value1 = ConstantValue(true)
      assert(value0.compare(value1, scope) == 0)
    }

    it("should support determine when Boolean values are not equivalent") {
      val value0 = ConstantValue(true)
      val value1 = ConstantValue(false)
      assert(value0.compare(value1, scope) != 0)
    }
  }

  describe("ConstantValue") {
    it("should support determine when Date values are equivalent") {
      val time = System.currentTimeMillis()
      val value0 = ConstantValue(time)
      val value1 = ConstantValue(time)
      assert(value0.compare(value1, scope) == 0)
    }

    it("should support determine when Date values are not equivalent") {
      val time = System.currentTimeMillis()
      val value0 = ConstantValue(time)
      val value1 = ConstantValue(time + 1000)
      assert(value0.compare(value1, scope) < 0)
      assert(value1.compare(value0, scope) > 0)
    }
  }

  describe("Expression") {
    it("should support comparisons") {
      val value0: Expression = (1: ConstantValue) + 2
      val value1: Expression = (8: ConstantValue) / 2
      assert(value0.compare(value1, scope) < 0)
      assert(value1.compare(value0, scope) > 0)
    }
  }

  describe("ConstantValue") {
    it("should support determine when Numeric values are equivalent") {
      val value0 = ConstantValue(100)
      val value1 = ConstantValue(100)
      assert(value0.compare(value1, scope) == 0)
    }

    it("should support determine when Numeric values are not equivalent") {
      val value0 = ConstantValue(100)
      val value1 = ConstantValue(1000)
      assert(value0.compare(value1, scope) < 0)
      assert(value1.compare(value0, scope) > 0)
    }
  }

  describe("ConstantValue") {
    it("should support determine when String values are equivalent") {
      val string = "Hello World"
      val value0 = ConstantValue(string)
      val value1 = ConstantValue(string)
      assert(value0.compare(value1, scope) == 0)
    }

    it("should support determine when String values are not equivalent") {
      val value0 = ConstantValue("Hello")
      val value1 = ConstantValue("World")
      assert(value0.compare(value1, scope) < 0)
      assert(value1.compare(value0, scope) > 0)
    }
  }

}
