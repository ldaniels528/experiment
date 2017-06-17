package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.QweryDecompiler._
import com.github.ldaniels528.qwery.ops._

/**
  * SQL Generator
  * @author lawrence.daniels@gmail.com
  */
class SQLGenerator {

  def insert(tableName: String, row: Row): String = {
    val columns = row.map(_._1)
    s"INSERT INTO $tableName (${
      columns.mkString(",")
    }) VALUES (${
      columns.indices.map(_ => "?").mkString(",")
    })"
  }

  def update(tableName: String, row: Row, where: Seq[String]): String = {
    val columns = row.map(_._1)
    s"UPDATE $tableName SET ${
      columns.map(name => s"$name=?").mkString(",")
    } WHERE ${
      where.map(name => s"$name=?").mkString(" AND ")
    }"
  }

  def update(tableName: String, row: Row, condition: Condition): String = {
    val columns = row.map(_._1)
    s"UPDATE $tableName SET ${
      columns.map(name => s"$name=?").mkString(",")
    } WHERE ${condition.toSQL}"
  }

}
