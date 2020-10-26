package com.qwery.database.types

import java.util.UUID

import com.qwery.database.types.QxAny._
import org.scalatest.funspec.AnyFunSpec

/**
 * Qx Test Suite
 */
class QxAnyTest extends AnyFunSpec {

  describe(classOf[QxAny].getName) {

    it("should work with Boolean types") {
      val b0: QxBoolean = true
      val b1: QxBoolean = false
      val b2: QxBoolean = QxFalse
      val b3: QxBoolean = QxTrue
      info(s"b0: $b0")
      info(s"b1: $b1")
      info(s"b2: $b2")
      info(s"b3: $b3")
    }

    it("should work with Int types") {
      val n: QxInt = 5
      val m = n + 3
      info(s"m >= 7? ${if (m >= 7) "Yes" else "No"} - m = $m")
    }

    it("should work with Double types") {
      val n: QxDouble = 5
      val m = n * 1.5
      info(s"m >= 7? ${if (m >= 7) "Yes" else "No"} - m = $m")
    }

    it("should work with String types") {
      val s0: QxString = "Hello"
      val s1: QxString = "World"
      val value = s0 + " " + s1
      info(s"value = $value")
    }

    it("should work with UUID types") {
      val u0: QxUUID = UUID.randomUUID()
      val u1: QxUUID = "12d57681-ef78-4086-84a2-dc6df94b0c7e"
      info(s"u0 = $u0")
      info(s"u1 = $u1")
    }

  }

}
