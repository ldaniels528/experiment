package com.qwery.util

import FormattingHelper._
import org.scalatest.FunSpec

/**
  * Formatting Helper Test
  * @author lawrence.daniels@gmail.com
  */
class FormattingHelperTest extends FunSpec {

  describe(FormattingHelper.getClass.getSimpleName) {

    it("should create human readable lists with 'and'") {
      val strings = Seq("apple", "orange", "grape").and()
      assert(strings == "apple, orange and grape")

      val symbols = Seq('apple, 'orange, 'grape).and()
      assert(symbols == "apple, orange and grape")
    }

    it("should create human readable lists with 'or'") {
      val strings = Seq("apple", "orange", "grape").or()
      assert(strings == "apple, orange or grape")

      val symbols = Seq('apple, 'orange, 'grape).or()
      assert(symbols == "apple, orange or grape")
    }

  }

}
