package com.github.ldaniels528.qwery.ops.builtins

import java.util.Date

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
      assert(Left("Hello World", 5).evaluate(scope).contains("Hello"))
    }
  }

  describe("Len") {
    it("should count the number of characters of a string") {
      assert(Len("Hello World").evaluate(scope).contains(11))
    }
  }

  describe("Now") {
    it("should return the current date/time") {
      assert(Now.evaluate(scope).exists(_.isInstanceOf[Date]))
    }
  }

  describe("PadLeft") {
    it("should right-justify the a string") {
      assert(PadLeft("Hello", 10).evaluate(scope).contains("     Hello"))
    }
  }

  describe("PadRight") {
    it("should left-justify the a string") {
      assert(PadRight("Hello", 10).evaluate(scope).contains("Hello     "))
    }
  }

  describe("Pow") {
    it("should left-justify the a string") {
      assert(Pow(2, 3).evaluate(scope).contains(8))
    }
  }

  describe("Right") {
    it("should extract a substring for N characters from right to left") {
      assert(Right("Hello World", 5).evaluate(scope).contains("World"))
    }
  }

  describe("Sign") {
    it("should determine the sign of a number") {
      assert(Sign(-123).evaluate(scope).contains(-1))
      assert(Sign(0).evaluate(scope).contains(0))
      assert(Sign(123).evaluate(scope).contains(1))
    }
  }

  describe("Split") {
    it("should split a string by a delimiting string") {
      assert(Split("Hello World", " ").evaluate(scope).contains(List("Hello", "World")))
    }
  }

  describe("Sqrt") {
    it("should compute the square root of a number") {
      assert(Sqrt(16).evaluate(scope).contains(4))
    }
  }

  describe("Substring") {
    it("should extract a substring for N characters starting at position M") {
      assert(Substring("Hello World", 1, 4).evaluate(scope).contains("ello"))
    }
  }

  describe("Trim") {
    it("should removes whitespace from the end of the string") {
      assert(Trim("Hello World  ").evaluate(scope).contains("Hello World"))
    }
  }

  describe("Uuid") {
    it("should removes whitespace from the end of the string") {
      assert(Uuid.evaluate(scope).exists(_.toString.matches("\\S{8}-\\S{4}-\\S{4}-\\S{4}-\\S{12}")))
    }
  }

}
