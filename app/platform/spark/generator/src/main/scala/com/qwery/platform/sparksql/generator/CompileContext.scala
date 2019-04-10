package com.qwery.platform.sparksql.generator

import com.qwery.models.{Invokable, TableLike}

/**
  * Compile Context
  * @author lawrence.daniels@gmail.com
  */
class CompileContext(tables: Seq[TableLike]) {
  private var imports: List[String] = Nil

  /**
    * Adds one or more class or package names for which to build imports
    * @param classNames the given class and/or package names
    */
  def addImports(classNames: String*): Unit = imports = imports ::: classNames.toList

  /**
    * Returns the class and package names
    * @return the class and package names
    */
  def getImports: List[String] = imports.distinct.reverse

  /**
    * Returns a table or view by its unique name
    * @param name the name of the desired [[TableLike table or view]]
    * @return the [[TableLike table or view]] or throws an [[IllegalArgumentException]] if not found
    */
  @throws[IllegalArgumentException]
  def lookupTableOrView(name: String): TableLike =
    tables.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Table or view '$name' was not found"))

}

/**
  * Compile Context Companion
  * @author lawrence.daniels@gmail.com
  */
object CompileContext {

  /**
    * Creates a new Compile Context instance
    * @param topLevelOp the given top-level [[Invokable]]
    * @return a new [[CompileContext]]
    */
  def apply(topLevelOp: Invokable): CompileContext = new CompileContext(
    tables = SparkCodeCompiler.findTablesAndViews(topLevelOp)
  )

}