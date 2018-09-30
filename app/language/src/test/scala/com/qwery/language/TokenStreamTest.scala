package com.qwery.language

import org.scalatest.FunSpec

/**
  * Token Stream Tests
  * @author lawrence.daniels@gmail.com
  */
class TokenStreamTest extends FunSpec {

  describe(classOf[TokenStream].getSimpleName) {

    it("should read ahead N tokens") {
      val ts = TokenStream("Now is the Winter of our discontent!")
      assert(ts is "now")
      assert(ts is "now is")
      assert(ts is "now is the Winter of our discontent !")
    }

    it("should match multiple tokens") {
      val ts = TokenStream("LEFT OUTER JOIN")
      assert((ts nextIf "LEFT OUTER JOIN") && ts.isEmpty)
    }

  }

}

