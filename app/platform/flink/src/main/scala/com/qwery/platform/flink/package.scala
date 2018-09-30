package com.qwery.platform

import org.apache.flink.table.api.Table
import org.apache.flink.types.Row

/**
  * flink package object
  * @author lawrence.daniels@gmail.com
  */
package object flink {

  type DataFrame = Table

  def die[A](message: String, cause: Throwable = null): A = throw new IllegalStateException(message, cause)

  /**
    * Table Extensions
    * @param table the given [[Table]]
    */
  final implicit class TableExtensions(val table: Table) extends AnyVal {
    @inline def print(n: Int = -1)(implicit rc: FlinkQweryContext): Unit = {
      import org.apache.flink.api.scala._
      rc.tableEnv.toAppendStream[Row](table).print()
    }
  }

}
