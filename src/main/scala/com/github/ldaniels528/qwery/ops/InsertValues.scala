package com.github.ldaniels528.qwery.ops

import scala.collection.Iterable
import scala.language.postfixOps

/**
  * Represents a collection of insert values
  * @author lawrence.daniels@gmail.com
  */
case class InsertValues(fields: Seq[Field], dataSets: Iterable[Seq[Expression]]) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    ResultSet(rows = dataSets map { dataSet =>
      fields zip dataSet map { case (field, value) =>
        field.name -> value.evaluate(scope)
          .getOrElse(throw new RuntimeException(s"Could not resolve value for '${field.name}': $value"))
      }
    } toIterator)
  }

}