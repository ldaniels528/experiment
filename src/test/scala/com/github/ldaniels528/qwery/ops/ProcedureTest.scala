package com.github.ldaniels528.qwery.ops

import java.io.File

import com.github.ldaniels528.qwery.ops.Implicits._
import com.github.ldaniels528.qwery.ops.sql.{Call, Insert, Procedure, Select}
import com.github.ldaniels528.qwery.sources.DataResource
import org.scalatest.FunSpec

/**
  * Procedure Test
  * @author Lawrence Daniels <lawrence.daniels@gmail.com>
  */
class ProcedureTest extends FunSpec {
  private val scope = RootScope()

  describe("Procedure") {

    it("executes a collection of queries") {
      val fields = List("Symbol", "Name", "Sector", "Industry", "LastSale").map(Field.apply)
      val procedure = Procedure(name = "test5", parameters = Nil, executable = CodeBlock(Seq(
        Insert(
          fields = fields,
          target = DataResource("test5.csv", hints = Some(Hints(append = Some(false)))),
          source = Select(
            fields = fields,
            source = Option(DataResource(path = "companylist.csv")),
            condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission"))
          )),
        Insert(
          fields = fields,
          target = DataResource("test5.json", hints = Some(Hints(append = Some(false)))),
          source = Select(
            fields = fields,
            source = Option(DataResource(path = "companylist.csv")),
            condition = Some(EQ(Field("Industry"), "Oil/Gas Transmission"))
          ))
      )))

      info("create the procedure")
      procedure.execute(scope)

      info("call the procedure")
      val resultSet = Call(name = "test5", args = Nil).execute(scope)

      info("4 records should have been inserted")
      assert(resultSet.rows.toSeq == Seq(Seq(("ROWS_INSERTED", 4))))

      Seq("test5.csv", "test5.json").map(new File(_)) foreach { file =>
        info(s"'${file.getName}' should exist")
        assert(file.exists())
      }
    }

  }

}
