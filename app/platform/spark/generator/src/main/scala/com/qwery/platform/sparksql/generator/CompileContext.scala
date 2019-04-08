package com.qwery.platform.sparksql.generator

import com.qwery.models.{Invokable, TableLike}

/**
  * Compile Context
  * @author lawrence.daniels@gmail.com
  */
class CompileContext(tables: Seq[TableLike]) {

  def lookupTableOrView(name: String): TableLike =
    tables.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Table or view '$name' was not found"))

}

/**
  * Compile Context Companion
  * @author lawrence.daniels@gmail.com
  */
object CompileContext {

  def apply(topLevelOp: Invokable): CompileContext = new CompileContext(
    tables = SparkCodeCompiler.findTablesAndViews(topLevelOp)
  )

}