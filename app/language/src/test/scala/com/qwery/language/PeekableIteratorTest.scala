package com.qwery.language

import org.scalatest.funspec.AnyFunSpec

/**
  * Peekable Iterator Test Suite
  * @author lawrence.daniels@gmail.com
  */
class PeekableIteratorTest extends AnyFunSpec {

  describe(classOf[PeekableIterator[_]].getSimpleName) {

    it("should reference elements by index") {
      val pit = new PeekableIterator(Seq("A", "B", "C"))
      assert(pit(0) contains "A")
      assert(pit(1) contains "B")
      assert(pit(2) contains "C")
    }

    it("should identify the index where a condition is satisfied") {
      val pit = new PeekableIterator(Seq("A", "B", "C"))
      assert(pit.indexWhereOpt(_ == "B") contains 1)
    }

    it("should allow looking ahead at values in the iterator") {
      val pit = new PeekableIterator(Seq("A", "B", "C"))
      assert(pit.peek contains "A")
      assert(pit.peekAhead(1) contains "B")
      assert(pit.peekAhead(2) contains "C")
    }

    it("should return the option of the next item from the iterator") {
      val pit = new PeekableIterator(Seq("A", "B", "C"))
      assert(pit.nextOption contains "A")
      assert(pit.nextOption contains "B")
      assert(pit.nextOption contains "C")
      assert(pit.nextOption.isEmpty)
    }

    it("should support mark and reset operations") {
      val pit = new PeekableIterator(Seq("A", "B", "C", "D"))
      assert(pit.next() == "A")
      pit.mark()
      assert(pit.index == 1)
      assert(pit.next() == "B")
      assert(pit.next() == "C")
      assert(pit.next() == "D")
      assertThrows[IllegalStateException](pit.next())
      pit.reset()
      assert(pit.next() == "B")
    }

    it("should identify the current element in toString()") {
      val pit = new PeekableIterator(Seq("A", "B", "C", "D", "E"))
      assert(pit.nextOption contains "A")
      assert(pit.nextOption contains "B")
      assert(pit.toString == "PeekableIterator(A, B, [C], D, E)")
    }

  }

}
