package com.github.ldaniels528.qwery.ops.sql

import java.util.Date

import com.github.ldaniels528.qwery.QweryDecompiler._
import com.github.ldaniels528.qwery.ops.{Executable, ResultSet, Row, Scope}

/**
  * Show Statement
  * @example {{{ select * from (show files) where Name like '%.csv'; }}}
  * @author lawrence.daniels@gmail.com
  */
case class Show(entityType: String) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    entityType.toUpperCase() match {
      case "FILES" =>
        ResultSet(scope.getFiles().map(file => Seq(
          "Name" -> file.getName,
          "Size" -> file.length,
          "LastModified" -> new Date(file.lastModified),
          "Path" -> file.getCanonicalFile.getParent
        ): Row).toIterator)

      case "FUNCTIONS" =>
        ResultSet(scope.getFunctions.map(function =>
          Seq("Name" -> function.name, "Value" -> function.executable.toSQL): Row).toIterator)

      case "PROCEDURES" =>
        ResultSet(scope.getProcedures.map(procedure =>
          Seq("Name" -> procedure.name, "Value" -> procedure.executable.toSQL): Row).toIterator)

      case "VARIABLES" =>
        ResultSet(scope.getVariables.map(variable =>
          Seq("Name" -> variable.name, "Value" -> variable.value): Row).toIterator)

      case "VIEWS" =>
        ResultSet(scope.getViews.map(view =>
          Seq("Name" -> view.name, "Value" -> view.query.toSQL): Row).toIterator)

      case unknown =>
        throw new IllegalArgumentException(s"Invalid entity type '$unknown'")
    }
  }

}

/**
  * Show Companion
  * @author lawrence.daniels@gmail.com
  */
object Show {
  private val entityTypes = Seq("FILES", "FUNCTIONS", "PROCEDURES", "VARIABLES", "VIEWS")

  def isValidEntityType(entityType: String): Boolean = entityTypes.exists(_.equalsIgnoreCase(entityType))

}
