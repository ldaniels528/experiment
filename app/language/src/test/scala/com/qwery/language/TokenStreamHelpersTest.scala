package com.qwery.language

import com.qwery.language.TokenStreamHelpers._
import com.qwery.util.StringHelper._
import org.scalatest.funspec.AnyFunSpec

/**
  * Token Stream Helpers Tests
  * @author lawrence.daniels@gmail.com
  */
class TokenStreamHelpersTest extends AnyFunSpec {

  describe(TokenStreamHelpers.getObjectSimpleName) {

    it("""should identify "100" as a constant""") {
      assert(TokenStream("100 = 1").isConstant)
    }

    it("""should identify "'Hello World'" as a constant""") {
      assert(TokenStream("'Hello World' = -1").isConstant)
    }

    it("""should identify "`Symbol`" as a field""") {
      assert(TokenStream("`Symbol` = 100").isField)
    }

    it("""should identify "PROPERTY_VAL" as a field""") {
      assert(TokenStream("PROPERTY_VAL = 100").isField)
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

    it("should support identifiers containing $ symbols") {
      assert(TokenStream("$$DATA_SOURCE_ID").isIdentifier)
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

