package com.qwery.platform.codegen.spark

import com.qwery.models.ColumnTypes.ColumnType
import com.qwery.models.StorageFormats.StorageFormat
import com.qwery.models._
import org.slf4j.LoggerFactory

/**
  * Code Generation Helper
  * @author lawrence.daniels@gmail.com
  */
object CodeGenerationHelper {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /**
    * Invokable Decoder Extensions
    * @param invokable the given [[Invokable]]
    */
  final implicit class InvokableDecoderExtensions(val invokable: Invokable) extends AnyVal {
    def decode: String = {
      logger.info(s"Decoding '$invokable'...")
      invokable match {
        case Create(t: Table) => s"""TableManager.add(${t.codify})"""
        case i: Insert => i.decode
        case m: MainProgram => m.code.decode
        case s: Select => s.decode
        case s: SQL => s.statements.map(stmt => stmt.decode).mkString("\n")
        case t: TableRef => t.name
        case x =>
          throw new IllegalArgumentException(s"Unsupported operation ${Option(x).map(_.getClass.getName).orNull}")
      }
    }
  }

  /**
    * Insert Decoder Extensions
    * @param insert the given [[Insert]]
    */
  final implicit class InsertDecoderExtensions(val insert: Insert) extends AnyVal {
    @inline def decode: String =
      s"""|TableManager.write(
          |   source = ${insert.source.decode},
          |   destination = TableManager("${insert.destination.target.decode}"),
          |   append = ${insert.destination.isInstanceOf[Insert.Into]}
          |)""".stripMargin
  }

  /**
    * Select Decoder Extensions
    * @param select the given [[Select]]
    */
  final implicit class SelectDecoderExtensions(val select: Select) extends AnyVal {
    def decode: String = {
      select.from.map(_.decode) match {
        case Some(inputPath) => s"""TableManager.read("$inputPath")"""
        case None => die(s"The data source for '$select'")
      }
    }
  }

  /**
    * String Codify Extensions
    * @param string the given [[String value]]
    */
  final implicit class StringCodifyExtension(val string: String) extends AnyVal {
    @inline def codify: String = s""""$string""""
  }

  /**
    * Storage Format Decoder Extensions
    * @param storageFormat the given [[StorageFormat]]
    */
  final implicit class StorageFormatExtensions(val storageFormat: StorageFormat) extends AnyVal {
    @inline def codify: String = s"StorageFormats.$storageFormat"
  }

  /**
    * Table Column Decoder Extensions
    * @param column the given [[Column]]
    */
  final implicit class TableColumnExtensions(val column: Column) extends AnyVal {
    import column._
    @inline def codify: String = s"""Column(name = "$name", `type` = ${`type`.codify}, isNullable = $isNullable)"""
  }

  /**
    * Table Column Type Decoder Extensions
    * @param columnType the given [[ColumnType]]
    */
  final implicit class TableColumnTypeExtensions(val columnType: ColumnType) extends AnyVal {
    @inline def codify: String = s"ColumnTypes.$columnType"
  }

  /**
    * Table Decoder Extensions
    * @param tableLike the given [[TableLike]]
    */
  final implicit class TableExtensions(val tableLike: TableLike) extends AnyVal {
    def codify: String = tableLike match {
      case table: Table =>
        import table._
        s"""|Table(
            |  name = "$name",
            |  columns = List(${columns.map(_.codify).mkString(",")}),
            |  location = "$location",
            |  fieldDelimiter = ${fieldDelimiter.map(_.codify)},
            |  fieldTerminator = ${fieldTerminator.map(_.codify)},
            |  headersIncluded = $headersIncluded,
            |  nullValue = ${nullValue.map(_.codify)},
            |  inputFormat = ${inputFormat.map(_.codify)},
            |  outputFormat = ${outputFormat.map(_.codify)},
            |  partitionColumns = List(${partitionColumns.map(_.codify).mkString(",")}),
            |  properties = Map(${properties map { case (k, v) => s""""$k" -> "$v""" } mkString ","}),
            |  serdeProperties = Map(${serdeProperties map { case (k, v) => s""""$k" -> "$v""" } mkString ","})
            |)""".stripMargin
      case table => die(s"Table type '${table.getClass.getSimpleName}' is not yet supported")
    }
  }

}
