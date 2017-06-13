package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.Executable

/**
  * Qwery Compiler
  * @author lawrence.daniels@gmail.com
  */
class QweryCompiler {

  /**
    * Compiles the given query (or statement) into an executable
    * @param sql the given query string (e.g. "SELECT * FROM './companylist.csv'")
    * @return an [[Executable executable]]
    */
  def apply(sql: String): Executable = compile(sql)

  /**
    * Compiles the given query (or statement) into an executable
    * @param sql the given query string (e.g. "SELECT * FROM './companylist.csv'")
    * @return an [[Executable executable]]
    */
  def compile(sql: String): Executable = SQLLanguageParser.parseNext(TokenStream(sql))

  /**
    * Compiles the given query (or statement) into an executable
    * @param sql the given query string (e.g. "SELECT * FROM './companylist.csv'")
    * @return an [[Executable executable]]
    */
  def compileFully(sql: String): Iterator[Executable] = SQLLanguageParser.parseFully(TokenStream(sql))

}

/**
  * Qwery Compiler Singleton
  * @author lawrence.daniels@gmail.com
  */
object QweryCompiler extends QweryCompiler