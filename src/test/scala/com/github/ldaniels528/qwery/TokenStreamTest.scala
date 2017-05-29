package com.github.ldaniels528.qwery

import org.scalatest.FunSpec

/**
  * Token Stream Tests
  * @author lawrence.daniels@gmail.com
  */
class TokenStreamTest extends FunSpec {

  describe("TokenStream") {

    it("can be ahead N tokens") {
      val ts = TokenStream("Now is the Winter of our discontent!")
      assert(ts is "now")
      assert(ts is "now is")
    }

  }

}
