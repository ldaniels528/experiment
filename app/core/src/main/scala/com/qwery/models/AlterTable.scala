package com.qwery.models

import com.qwery.models.AlterTable.Alteration

/**
 * Represents an ALTER TABLE
 * @example {{{
 * ALTER TABLE stocks ADD COLUMN comments TEXT DEFAULT ''
 * }}}
 * @example {{{
 * ALTER TABLE stocks DROP COLUMN comments
 * }}}
 * @param ref         the [[EntityRef table reference]]
 * @param alterations the collection of [[Alteration alterations]]
 */
case class AlterTable(ref: EntityRef, alterations: Seq[Alteration]) extends Invokable

/**
 * Alter Table Companion
 */
object AlterTable {

  /**
   * Creates a new ALTER TABLE operation
   * @param ref  the [[EntityRef table reference]]
   * @param alteration the [[Alteration alteration]]
   * @return a new [[AlterTable ALTER TABLE operation]]
   */
  def apply(ref: EntityRef, alteration: Alteration) = new AlterTable(ref, alterations = Seq(alteration))

  /**
   * Represents a table alteration
   */
  sealed trait Alteration extends Invokable {
    def toSQL: String
  }

  /**
   * Represents an alteration to add a column
   * @param column the [[Column]] to add the next position in the table
   */
  case class AddColumn(column: Column) extends Alteration {
    override def toSQL = s"ADD COLUMN ${column.toSQL}"
  }

  /**
   * Represents an alteration to append a column
   * @param column the [[Column]] to append the end of the table
   */
  case class AppendColumn(column: Column) extends Alteration {
    override def toSQL = s"APPEND COLUMN ${column.toSQL}"
  }

  /**
   * Represents an alteration to remove a column
   * @param columnName the name of the column to remove from the table
   */
  case class DropColumn(columnName: String) extends Alteration {
    override def toSQL = s"DROP COLUMN $columnName"
  }

  /**
   * Represents an alteration to add a column
   * @param column the [[Column]] to prepend to the first position in the table
   */
  case class PrependColumn(column: Column) extends Alteration {
    override def toSQL = s"PREPEND COLUMN ${column.toSQL}"
  }

}