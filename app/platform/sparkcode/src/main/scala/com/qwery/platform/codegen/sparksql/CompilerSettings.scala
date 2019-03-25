package com.qwery.platform.codegen.sparksql

/**
  * Compiler Settings
  * @param defaultDB the default database name
  * @param inlineSQL indicates whether to generate inline SQL
  */
case class CompilerSettings(defaultDB: String = "global_temp",
                            inlineSQL: Boolean = true)