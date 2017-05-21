package com.github.ldaniels528.qwery.sources

import org.scalatest.FunSpec

/**
  * Input Source Tests
  * @author lawrence.daniels@gmail.com
  */
class InputSourceTest extends FunSpec {

  describe("InputSource") {

    it("should iterate over data") {
      InputSource("companylist.csv") foreach { source =>
        source.open()
        var count = 0L
        source.toIterator foreach { row =>
          assert(row.size == 9)
          count += 1
        }
        source.close()

        assert(count == 359)
      }
    }
  }

}
