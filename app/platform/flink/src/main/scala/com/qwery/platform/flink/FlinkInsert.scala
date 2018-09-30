package com.qwery.platform.flink

import com.qwery.models.Insert.DataRow
import com.qwery.models.Location
import com.qwery.models.expressions.Expression

/**
  * Represents an Insert operation
  * @param destination the given [[FlinkInvokable destination]]
  * @param source      the given [[FlinkInvokable source]]
  * @param fields      the fields to insert into the table
  */
case class FlinkInsert(destination: FlinkInvokable, source: FlinkInvokable, fields: Seq[Expression]) extends FlinkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] =
    destination.execute(source.execute(input))
}

/**
  * Insert Operation Companion
  * @author lawrence.daniels@gmail.com
  */
object FlinkInsert {

  /**
    * Represents a writable sink
    * @param target the given [[Location target]]
    * @param append indicates whether the data should be appended
    */
  case class Sink(target: Location, append: Boolean) extends FlinkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = input map { df =>
      FlinkQweryCompiler.write(df, target, append)
      df
    }
  }

  /**
    * Represents a readable spout
    * @param values the given readable values
    * @param target the optional target [[Location]]
    */
  case class Spout(values: List[DataRow], target: Option[Location] = None) extends FlinkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
      import FlinkQweryCompiler.Implicits._
      target map { ref =>
        import org.apache.flink.api.scala._
        val columns = ref.resolveColumns
        val rows = values.map(_.map(_.asAny))
        val dataStream = rc.env.fromCollection(rows)
        rc.tableEnv.fromDataStream(dataStream)
      }
    }
  }

}