package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.Executable

/**
  * Qwery Compiler
  * @author lawrence.daniels@gmail.com
  */
class QweryCompiler {

  /**
    * Compiles the given query (or statement) into an executable
    * @param query the given query string (e.g. "SELECT * FROM './companylist.csv'")
    * @return an [[Executable executable]]
    */
  def apply(query: String): Executable = compile(query)

  /**
    * Compiles the given query (or statement) into an executable
    * @param query the given query string (e.g. "SELECT * FROM './companylist.csv'")
    * @return an [[Executable executable]]
    */
  def compile(query: String): Executable = SQLLanguageParser.parseNext(TokenStream(query))

}

/**
  * Qwery Compiler Singleton
  * @author lawrence.daniels@gmail.com
  */
object QweryCompiler extends QweryCompiler