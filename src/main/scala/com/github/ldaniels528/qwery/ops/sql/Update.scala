package com.github.ldaniels528.qwery.ops.sql

import com.github.ldaniels528.qwery.ops.{Executable, Expression, Field, ResultSet, Row, Scope}
import com.github.ldaniels528.qwery.sources.{DataResource, JDBCSupport}
import com.github.ldaniels528.qwery.util.ResourceHelper._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
  * Represents an UPDATE statement
  * @author lawrence.daniels@gmail.com
  */
case class Update(target: DataResource, assignments: Seq[(String, Expression)], source: Executable, keyedOn: Seq[Field])
  extends Executable with JDBCSupport {
  private lazy val log = LoggerFactory.getLogger(getClass)
  private var offset = 0L

  override def execute(scope: Scope): ResultSet = {
    var updated = 0L
    val outputSource = getJDBCOutputSource(target, scope)
    outputSource.open(scope)
    outputSource use { device =>
      source.execute(scope) foreach { row =>
        offset += 1

        // build the updated row
        val mappings = Map(assignments.flatMap { case (name, expr) => expr.evaluate(scope).map(name -> _) }: _*)
        val updateRow: Row = row map { case (name, value) =>
          name -> mappings.getOrElse(name, value)
        }

        // perform the update
        device.update(updateRow, keyedOn.map(_.name)) match {
          case Success(results) => results.foreach { case (_, updates) => updated += updates }
          case Failure(e) =>
            log.error(s"JDBC Update # $offset failed: ${e.getMessage}")
            log.error(s"Record: [${updateRow map { case (k,v) => s"$k:'$v'" } mkString ","}]")
        }
      }
      ResultSet.updated(updated = updated, statistics = outputSource.getStatistics)
    }

  }

}