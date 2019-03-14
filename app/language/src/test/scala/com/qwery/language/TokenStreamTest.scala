package com.qwery.language

import com.qwery.language.TokenStreamHelpers._
import org.scalatest.FunSpec

/**
  * Token Stream Tests
  * @author lawrence.daniels@gmail.com
  */
class TokenStreamTest extends FunSpec {

  describe(classOf[TokenStream].getSimpleName) {

    it("""should identify "100" as a constant""") {
      assert(TokenStream("100 = 1").isConstant)
    }

    it("""should identify "'Hello World'" as a constant""") {
      assert(TokenStream("'Hello World' = -1").isConstant)
    }

    it("""should identify "`Symbol`" as a field""") {
      assert(TokenStream("`Symbol` = 100").isField)
    }

    it("""should identify "Symbol" as a field""") {
      assert(TokenStream("Symbol = 100").isField)
    }

    it("""should identify "A.Symbol" as a JOIN column""") {
      assert(TokenStream("A.Symbol = 100").isJoinColumn)
    }

    it("""should identify "Sum(A.Symbol)" as a function""") {
      assert(TokenStream("Sum(A.Symbol) = 100").isFunction)
    }

    it("""should NOT identify "ABC + (1 * x)" as a function""") {
      assert(!TokenStream("ABC + (1 * x)").isFunction)
    }

    it("should match multiple tokens (is)") {
      val ts = TokenStream("Now is the Winter of our discontent!")
      assert(ts is "now")
      assert(ts is "now is")
      assert(ts is "now is the Winter of our discontent !")
    }

    it("should match multiple tokens (nextIf)") {
      val ts = TokenStream("LEFT OUTER JOIN")
      assert((ts nextIf "LEFT OUTER JOIN") && ts.isEmpty)
    }

  }

}

