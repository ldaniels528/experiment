package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.Row
import org.scalatest.FunSpec

/**
  * SQL Generator Test
  * @author lawrence.daniels@gmail.com
  */
class SQLGeneratorTest extends FunSpec {
  private val sqlGenerator = new SQLGenerator()

  describe("SQLGenerator") {

    it("should generate INSERT statements") {
      val row: Row = Seq("a" -> 1, "b" -> 2, "c" -> 3)
      val sql = sqlGenerator.insert("alphabetSoup", row)
      info(s"sql: $sql")
      assert(sql == "INSERT INTO alphabetSoup (a,b,c) VALUES (?,?,?)")
    }

    it("should generate UPDATE statements") {
      val row: Row = Seq("a" -> 1, "b" -> 2, "c" -> 3)
      val sql = sqlGenerator.update("alphabetSoup", row, where = Seq("a", "b"))
      info(s"sql: $sql")
      assert(sql == "UPDATE alphabetSoup SET a=?,b=?,c=? WHERE a=? AND b=?")
    }
  }

}
