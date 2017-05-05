package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.{Field, ResultSet}

import scala.collection.Iterable

/**
  * Represents a collection of record modifications
  * @author lawrence.daniels@gmail.com
  */
case class Modifications(fields: Seq[Field], dataSets: Iterable[Seq[Any]]) extends Executable {

  override def execute(): ResultSet = {
    dataSets map { dataSet =>
      fields zip dataSet map { case (field, value) =>
        field.name -> value
      }
    }
  }

}