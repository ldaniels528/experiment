package com.github.ldaniels528.qwery.ops.sql

import com.github.ldaniels528.qwery.ops.{Executable, Field, ResultSet, Row, Scope}
import com.github.ldaniels528.qwery.sources.{DataResource, JDBCSupport}
import com.github.ldaniels528.qwery.util.ResourceHelper._

/**
  * Represents an UPSERT statement
  * @author lawrence.daniels@gmail.com
  */
case class Upsert(target: DataResource, fields: Seq[Field], source: Executable, keyedOn: Seq[Field])
  extends Executable with JDBCSupport {

  override def execute(scope: Scope): ResultSet = {
    var inserted = 0L
    var updated = 0L
    val outputSource = getJDBCOutputSource(target, scope)
    outputSource.open(scope)
    outputSource use { device =>
      source.execute(scope) foreach { row =>
        val upsertRow: Row = fields zip row map { case (field, (_, value)) =>
          field.name -> value
        }
        device.upsert(upsertRow, keyedOn.map(_.name)) foreach { case (inserts, updates) =>
          inserted += inserts
          updated += updates
        }
      }
    }
    ResultSet.upserted(inserted = inserted, updated = updated, statistics = outputSource.getStatistics)
  }

}