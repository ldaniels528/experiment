package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Hints, RootScope}
import com.github.ldaniels528.qwery.util.OptionHelper.Risky._
import org.scalatest.FunSpec

/**
  * Input Source Tests
  * @author lawrence.daniels@gmail.com
  */
class InputSourceTest extends FunSpec {
  private val scope = RootScope()

  describe("InputSource") {

    it("should auto-detect the delimiter and iterate over data") {
      InputSource("companylist.csv") foreach { source =>
        source.open(scope)
        assert(source.toIterator(scope).count { row =>
          assert(row.size == 9)
          true
        } == 359) // 360 - 1 (header)
      }
    }

    it("should iterate over data with an explicit format") {
      InputSource("companylist.csv", hints = Hints().asCSV) foreach { source =>
        source.open(scope)
        assert(source.toIterator(scope).count { row =>
          assert(row.size == 9)
          true
        } == 359) // 360 - 1 (header)
      }
    }
  }

}
