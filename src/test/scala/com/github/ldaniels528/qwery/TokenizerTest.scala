package com.github.ldaniels528.qwery

import org.scalatest.FunSpec

/**
  * Tokenizer Test
  * @author lawrence.daniels@gmail.com
  */
class TokenizerTest extends FunSpec {

  describe("Tokenizer") {

    it("support parsing alphanumeric text") {
      val tok = Tokenizer("Hello World")
      while (tok.hasNext) {
        info(s"tok: ${tok.next()}")
      }
    }

    it("support parsing quoted text") {
      val tok = Tokenizer("'Hello World' \"Text in text\"")
      while (tok.hasNext) {
        info(s"tok: ${tok.next()}")
      }
    }

    it("support parsing numeric text") {
      val tok = Tokenizer("112.3+1111.2*12/3")
      while (tok.hasNext) {
        info(s"tok: ${tok.next()}")
      }
    }

  }

}
