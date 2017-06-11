package com.github.ldaniels528.qwery.ops

import java.util.Date

import com.github.ldaniels528.qwery.QwerySQLConversion._

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
        )).toIterator)

      case "PROCEDURES" =>
        ResultSet(scope.getProcedures.map(procedure =>
          Seq("Name" -> procedure.name, "Value" -> procedure.executable.toSQL)).toIterator)

      case "VARIABLES" =>
        ResultSet(scope.getVariables.map(variable =>
          Seq("Name" -> variable.name, "Value" -> variable.value)).toIterator)

      case "VIEWS" =>
        ResultSet(scope.getViews.map(view =>
          Seq("Name" -> view.name, "Value" -> view.query.toSQL)).toIterator)

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
  private val entityTypes = Seq("FILES", "PROCEDURES", "VARIABLES", "VIEWS")

  def isValidEntityType(entityType: String): Boolean = entityTypes.exists(_.equalsIgnoreCase(entityType))

}
