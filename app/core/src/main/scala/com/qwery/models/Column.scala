package com.qwery
package models

/**
 * Represents a logical column having a yet-to-be realized content type
 * @param name         the column name
 * @param spec         the logical [[ColumnTypeSpec column type]]
 * @param comment      the optional comment/remarks
 * @param defaultValue the optional default value
 * @param enumValues   the enumeration values (if any)
 * @param isCompressed indicates whether the data is to be compressed
 * @param isNullable   indicates whether the column may contain nulls
 * @param isRowID      indicates whether the column returns the row ID
 */
case class Column(name: String,
                  spec: ColumnTypeSpec,
                  comment: Option[String] = None,
                  defaultValue: Option[String] = None,
                  enumValues: Seq[String] = Nil,
                  isCompressed: Boolean = false,
                  isNullable: Boolean = true,
                  isRowID: Boolean = false) {

  /**
   * @return true, if the column is an enumeration type
   */
  @inline def isEnum: Boolean = enumValues.nonEmpty

  override def toString: String = {
    f"""|${getClass.getSimpleName}(
        |name=$name,
        |spec=$spec,
        |comment=${comment.orNull},
        |defaultValue=${defaultValue.orNull},
        |enumValues=[${enumValues.map(s => s"'$s'").mkString(",")}],
        |isCompressed=$isCompressed,
        |isNullable=$isNullable,
        |isRowID=$isRowID
        |)""".stripMargin.split("\n").mkString
  }

}

/**
  * Column Companion
  */
object Column {

  /**
    * Constructs a new column from the given descriptor
    * @param descriptor the column descriptor (e.g. "symbol string true")
    * @return a new [[Column]]
    */
  def apply(descriptor: String): Column = descriptor.split("[ ]").toList match {
    case name :: _type :: nullable :: Nil =>
      new Column(name, spec = new ColumnTypeSpec(_type.toUpperCase), isNullable = nullable.toBoolean)
    case name :: _type :: Nil =>
      new Column(name, spec = new ColumnTypeSpec(_type.toUpperCase))
    case unknown =>
      die(s"Invalid column descriptor '$unknown'")
  }

}
