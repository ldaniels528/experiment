package com.github.ldaniels528.qwery.ops.sql

import com.github.ldaniels528.qwery.QweryDecompiler.ConditionExtensions
import com.github.ldaniels528.qwery.ops.{Condition, Executable, Field, ResultSet, Row, Scope}
import com.github.ldaniels528.qwery.sources.{DataResource, JDBCOutputSource}
import com.github.ldaniels528.qwery.util.ResourceHelper._

/**
  * Represents an UPSERT statement
  * @author lawrence.daniels@gmail.com
  */
case class Upsert(target: DataResource, fields: Seq[Field], source: Executable, keyedOn: Seq[Field]) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    var count = 0L
    val outputSource = getJDBCOutputSource(scope)
    outputSource.open(scope)
    outputSource use { device =>
      source.execute(scope) foreach { row =>
        val upsertRow: Row = fields zip row map { case (field, (_, value)) =>
          field.name -> value
        }
        device.upsert(upsertRow, keyedOn.map(_.name))
        count += 1
      }
    }
    ResultSet.inserted(count, statistics = outputSource.getStatistics)
  }

  private def getJDBCOutputSource(scope: Scope): JDBCOutputSource = {
    target.getOutputSource(scope) match {
      case Some(source: JDBCOutputSource) => source
      case Some(_) =>
        throw new IllegalArgumentException("Only JDBC Output Sources support UPSERT")
      case None =>
        throw new IllegalStateException(s"No output source found for '${target.path}'")
    }
  }

  private def toWhereClause(condition: Condition): String = {
    condition.toSQL
  }

}