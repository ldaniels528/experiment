package com.qwery.database.models

import com.qwery.database.DatabaseCPU.Solution
import com.qwery.database.device.BlockDevice
import com.qwery.models.TableRef
import org.slf4j.Logger

case class QueryResult(ref: TableRef, columns: Seq[Column] = Nil, rows: Seq[Seq[Option[Any]]] = Nil, count: Long = 0, __ids: Seq[Long] = Nil) {

  def foreachKVP(f: KeyValues => Unit): Unit = {
    val columnNames = columns.map(_.name)
    rows foreach { values =>
      val kvp = KeyValues((columnNames zip values).flatMap { case (key, value_?) => value_?.map(value => key -> value) }: _*)
      f(kvp)
    }
  }

  def show(limit: Int = 20)(implicit logger: Logger): Unit = {
    logger.info(columns.map(c => f"${c.name}%-12s").mkString(" | "))
    logger.info(columns.map(_ => "-" * 12).mkString("-+-"))
    for (row <- rows.take(limit)) logger.info(row.map(v => f"${v.orNull}%-12s").mkString(" | "))
  }

}

object QueryResult {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._
  implicit val queryResultJsonFormat: RootJsonFormat[QueryResult] = jsonFormat5(QueryResult.apply)

  final implicit class SolutionToQueryResult(val solution: Solution) extends AnyVal {

    @inline
    def toQueryResult: QueryResult = {
      val (columns, rows, count: Long) = solution.get match {
        case Left(device) => (device.columns, device.toList, 0L)
        case Right(count) => (Nil, Nil, count)
      }
      QueryResult(
        solution.ref,
        columns = columns,
        __ids = rows.map(_.id),
        count = count,
        rows = rows map { row =>
          val mapping = row.toMap
          columns.map(_.name).map(mapping.get)
        })
    }

  }

  final implicit class BlockDeviceToQueryResult(val device: BlockDevice) extends AnyVal {

    @inline
    def toQueryResult(ref: TableRef): QueryResult = {
      val rows = device.toList
      val columns = device.columns
      QueryResult(
        ref,
        columns = columns,
        __ids = rows.map(_.id),
        count = rows.size,
        rows = rows map { row =>
          val mapping = row.toMap
          columns.map(_.name).map(mapping.get)
        })
    }

  }

}