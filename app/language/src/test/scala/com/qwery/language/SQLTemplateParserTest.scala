package com.qwery.language

import com.qwery.models._
import com.qwery.models.expressions._
import ColumnSpec.implicits._
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec

import scala.util.{Failure, Success, Try}

/**
  * SQL Template Parser Test
  * @author lawrence.daniels@gmail.com
  */
class SQLTemplateParserTest extends AnyFunSpec {

  describe(classOf[SQLTemplateParser].getSimpleName) {
    import com.qwery.models.expressions.implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    it("should parse argument tags (%A)") {
      verify(text = "(1,2,3)", template = "%A:args")(SQLTemplateParams(expressions = Map("args" -> List(1, 2, 3))))
    }

    it("should parse atom tags (%a)") {
      verify(text = "Customers", template = "%a:target")(SQLTemplateParams(atoms = Map("target" -> "Customers")))
    }

    it("should parse chooser tags (%C)") {
      verify(text = "INTO", template = "%C(mode|INTO|OVERWRITE)")(SQLTemplateParams(atoms = Map("mode" -> "INTO")))
      verify(text = "OVERWRITE", template = "%C(mode|INTO|OVERWRITE)")(SQLTemplateParams(atoms = Map("mode" -> "OVERWRITE")))
    }

    it("should parse condition tags (%c)") {
      verify(text = "custId = 123", template = "%c:condition")(SQLTemplateParams(conditions = Map("condition" -> (Field('custId) === 123d))))
    }

    it("should parse expression tags (%E)") {
      verify(text = "field1, 'hello', 5", template = "%E:fields")(SQLTemplateParams(expressions = Map("fields" -> List('field1, "hello", 5.0))))
    }

    it("should parse assignable expression tags (%e)") {
      verify(text = "(x + 1) * 2", template = "%e:expression")(SQLTemplateParams(assignables = Map("expression" -> ((Field('x) + 1) * 2))))
    }

    it("should parse field tags (%F)") {
      verify(text = "field1, field2, field3", template = "%F:fields")(SQLTemplateParams(fields = Map("fields" -> List('field1, 'field2, 'field3))))
    }

    it("should parse storage format tags (%f)") {
      verify(text = "INPUTFORMAT 'CSV' OUTPUTFORMAT 'JSON'", template = "%f:formats")(SQLTemplateParams(atoms = Map("formats.input" -> "CSV", "formats.output" -> "JSON")))
      verify(text = "OUTPUTFORMAT 'CSV' INPUTFORMAT 'JSON'", template = "%f:formats")(SQLTemplateParams(atoms = Map("formats.input" -> "JSON", "formats.output" -> "CSV")))
    }

    it("should parse join tags (%J)") {
      verify(text = "INNER JOIN Securities AS A ON A.symbol = B.ticker", template = "%J:joins")(SQLTemplateParams(joins = Map("joins" -> List(
        Join(source = Table("Securities").as("A"), condition = Field("A.symbol") === Field("B.ticker"), `type` = JoinTypes.INNER)
      ))))
    }

    it("should parse keyword tags (%k)") {
      verify(text = "LOCATION", template = "%k:LOCATION")(SQLTemplateParams(keywords = Set("LOCATION")))
      verify(text = "FROM", template = "%k:LOCATION")(SQLTemplateParams(keywords = Set.empty))
    }

    it("should parse location tags (%L)") {
      verify(text = "TABLE assets", template = "%L:table")(SQLTemplateParams(locations = Map("table" -> Table("assets"))))
      verify(text = "TABLE `the assets`", template = "%L:table")(SQLTemplateParams(locations = Map("table" -> Table("the assets"))))
      verify(text = "assets", template = "%L:table")(SQLTemplateParams(locations = Map("table" -> Table("assets"))))
      verify(text = "`the assets`", template = "%L:table")(SQLTemplateParams(locations = Map("table" -> Table("the assets"))))
    }

    it("should parse numeric tags (%n)") {
      verify(text = "100", template = "%n:limit")(SQLTemplateParams(numerics = Map("limit" -> 100d)))
    }

    it("should parse ordered field tags (%o)") {
      verify(text = "field1 DESC, field2 ASC", template = "%o:orderedFields")(SQLTemplateParams(orderedFields = Map(
        "orderedFields" -> List(OrderColumn("field1", isAscending = false), OrderColumn("field2", isAscending = true))
      )))
    }

    it("should parse parameter tags (%P)") {
      verify(text = "name STRING(32), age INTEGER, dob DATE", template = "%P:params")(SQLTemplateParams(columns = Map(
        "params" -> List(
          Column(name = "name", spec = ColumnSpec("STRING", precision = List(32))),
          Column(name = "age", "INTEGER"),
          Column(name = "dob", "DATE"))
      )))
    }

    it("should parse properties tags (%p)") {
      verify(text = "('skip.header.line.count'='1', 'transient_lastDdlTime'='1548444883')", template = "%p:props")(SQLTemplateParams(properties = Map(
        "props" -> Map("skip.header.line.count" -> "1", "transient_lastDdlTime"-> "1548444883")
      )))
    }

    it("should parse direct query tags (%Q)") {
      verify(text = "SELECT firstName, lastName FROM AddressBook", template = "%Q:query")(SQLTemplateParams(sources = Map(
        "query" -> Select(fields = List('firstName, 'lastName), from = Table("AddressBook"))
      )))
      verifyNot(text = "AddressBook", template = "%Q:table")(failure = "Expected keyword CALL or SELECT near 'AddressBook'")
      verify(text = "#addressBook", template = "%Q:variable")(SQLTemplateParams(sources = Map(
        "variable" -> @#("addressBook")
      )))
    }

    it("should parse query source (queries, tables and variables) tags (%q)") {
      verify(text = "( SELECT firstName, lastName FROM AddressBook )", template = "%q:query")(SQLTemplateParams(sources = Map(
        "query" -> Select(fields = List('firstName, 'lastName), from = Table("AddressBook"))
      )))
      verify(text = "AddressBook", template = "%q:table")(SQLTemplateParams(sources = Map(
        "table" -> Table("AddressBook")
      )))
      verify(text = "#addressBook", template = "%q:variable")(SQLTemplateParams(sources = Map(
        "variable" -> @#("addressBook")
      )))
    }

    it("should parse repeated sequence tags (%R)") {
      verify(text = "(123, 456) (345, 678)", template = "%R:valueSet {{ ( %E:values ) }}")(
        SQLTemplateParams(repeatedSets = Map("valueSet" -> List(
          SQLTemplateParams(expressions = Map("values" -> List(123.0, 456.0))),
          SQLTemplateParams(expressions = Map("values" -> List(345.0, 678.0)))
        ))))
    }

    it("should parse regular expression tags (%r)") {
      verify(text = "'123ABC'", template = "%r`\\d{3,4}\\S+`")(SQLTemplateParams())
    }

    it("should parse table tags (%t)") {
      verify(text = "`Customers`", template = "%t:target")(SQLTemplateParams(atoms = Map("target" -> "Customers")))
      verify(text = "Customers", template = "%t:target")(SQLTemplateParams(atoms = Map("target" -> "Customers")))
    }

    it("should parse key-value-pairs tags (%U)") {
      verify(text = "comments = 'Raise the price'", template = "%U:assignments")(SQLTemplateParams(keyValuePairs = Map(
        "assignments" -> List("comments" -> Literal("Raise the price"))
      )))
    }

    it("should parse insert values (queries, VALUES and variables) tags (%V)") {
      verify(text = "( SELECT * FROM AddressBook )", template = "%V:query")(SQLTemplateParams(sources = Map(
        "query" -> Select(fields = List('*), from = Table("AddressBook"))
      )))
      verify(text = "VALUES (1, 2, 3)", template = "%V:values")(SQLTemplateParams(sources = Map(
        "values" -> Insert.Values(List(List(1d, 2d, 3d)))
      )))
      verify(text = "#addressBook", template = "%V:variable")(SQLTemplateParams(sources = Map(
        "variable" -> @#("addressBook")
      )))
    }

    it("should parse variable reference tags (%v)") {
      verify(text = "#variable", template = "%v:variable")(SQLTemplateParams(variables = Map(
        "variable" -> @#("variable")
      )))
    }

    it("should parse optionally required tags (?, +?)") {
      verify(text = "LIMIT 100", template = "?LIMIT +?%n:limit")(SQLTemplateParams(numerics = Map("limit" -> 100d)))
      verifyNot(text = "LIMIT AAA", template = "?LIMIT +?%n:limit")(failure = "'limit' expected")
      verify(text = "NOLIMIT 100", template = "?LIMIT +?%n:limit")(SQLTemplateParams())
    }

  }

  def verify(text: String, template: String)(expected: SQLTemplateParams): Assertion = {
    info(s"'$template' <~ '$text'")
    val actual = SQLTemplateParams(TokenStream(text), template)
    println(s"actual:   ${actual.columns}")
    println(s"expected: ${expected.columns}")
    assert(actual == expected, s"'$text' ~> '$template' failed")
  }

  def verifyNot(text: String, template: String)(failure: String): Assertion = {
    Try(SQLTemplateParams(TokenStream(text), template)) match {
      case Success(_) => fail("Negative test failure")
      case Failure(e) =>
        info(s"'$template' <!~ '$text' [${e.getMessage}]")
        assert(e.getMessage.contains(failure), s"'$text' ~> '$template' failed")
    }
  }

}
