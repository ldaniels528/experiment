package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.QueryCompiler._
import com.github.ldaniels528.qwery.sources._

import scala.collection.mutable

/**
  * Query Compiler
  * @author lawrence.daniels@gmail.com
  */
class QueryCompiler {
  private val keyWords = Seq(
    "FROM", "GROUP", "INSERT", "INTO", "LIMIT", "ORDER", "SELECT", "VALUES", "WHERE"
  )

  /**
    * Compiles the given query (or statement) into an executable
    * @param query the given query string (e.g. "SELECT * FROM './companylist.csv'")
    * @return an [[Executable executable]]
    */
  def apply(query: String): Executable = compile(Tokenizer(query))

  /**
    * Compiles the given query (or statement) into an executable
    * @param query the given query string (e.g. "SELECT * FROM './companylist.csv'")
    * @return an [[Executable executable]]
    */
  def compile(query: String): Executable = compile(Tokenizer(query))

  /**
    * Compiles the given query (or statement) into an executable
    * @param tok the given [[Tokenizer]] of elements
    * @return an [[Executable executable]]
    */
  def compile(tok: Tokenizer): Executable = {
    val mapping = buildKeyWordMapping(tok)

    // process the command
    mapping.headOption match {
      case Some((word, args)) if word.equalsIgnoreCase("INSERT") & mapping.containsIgnoreCase("SELECT") =>
        parseInsertSelect(args, mapping.toMap)
      case Some((word, args)) if word.equalsIgnoreCase("INSERT") => parseInsert(mapping.toMap)
      case Some((word, args)) if word.equalsIgnoreCase("SELECT") => parseSelect(args, mapping.toMap)
      case Some((unknown, args)) =>
        throw new SyntaxException(s"Unrecognized keyword '$unknown'", args.headOption.orNull)
      case None =>
        throw new SyntaxException("Unexpected end of line")
    }
  }

  private def parseInsert(mapping: Map[String, Seq[Token]]): Insert = {
    // INSERT INTO <target> (<fields>) VALUES (<values>)
    throw new IllegalStateException("INSERT-VALUES is not yet implemented")
  }

  private def parseInsertSelect(args: Seq[Token], mapping: Map[String, Seq[Token]]): InsertSelect = {
    // INSERT INTO <target> (<fields>) SELECT <fields> FROM <source> WHERE <conditions> LIMIT <count>
    val (target, intoFields) = mapping.getIgnoreCase("INTO") match {
      case Some(intoArgs) =>
        val it = intoArgs.toIterator
        val targetArgs = it.takeWhile(_.text != "(").toSeq
        val fieldArgs = it.takeWhile(_.text != ")").toSeq
        (parseTarget(targetArgs), parseFields(fieldArgs))
      case None => throw new SyntaxException("Keyword 'INTO' expected", args.headOption.orNull)
    }
    val selectFields = mapping.getIgnoreCase("SELECT").orNull
    InsertSelect(target, intoFields, parseSelect(selectFields, mapping.toMap))
  }

  /**
    * Builds a mapping of keywords to arguments
    * @param tok the given [[Tokenizer token iteration]]
    * @return a mapping of keywords to arguments
    */
  private def buildKeyWordMapping(tok: Tokenizer) = {
    val mapping = mutable.LinkedHashMap[String, mutable.ListBuffer[Token]]()
    var lastKeyWord: Option[String] = None
    while (tok.hasNext) {
      val token = tok.next()
      if (keyWords.contains(token.text)) {
        lastKeyWord = Some(token.text)
        mapping.getOrElseUpdate(token.text, mutable.ListBuffer())
      }
      else {
        lastKeyWord.foreach { kw =>
          val buf = mapping.getOrElseUpdate(kw, mutable.ListBuffer())
          buf += token
        }
      }
    }
    mapping
  }

  private def parseSelect(args: Seq[Token], mapping: Map[String, Seq[Token]]): Query = {
    Selection(
      source = mapping.getIgnoreCase("FROM").flatMap(parseSource),
      fields = parseFields(args),
      condition = mapping.getIgnoreCase("WHERE").flatMap(args => parseExpressions(args.iterator)),
      limit = mapping.getIgnoreCase("LIMIT").flatMap(args => parseLimit(args)))
  }

  private def parseExpressions(tok: Iterator[Token]): Option[Expression] = {
    var expression: Option[Expression] = None
    while (tok.hasNext) {
      // continued expression? ... AND field = 1
      if (expression.nonEmpty) {
        tok.next() match {
          case AlphaToken(opr, _) if opr.equalsIgnoreCase("AND") =>
            expression = expression.map(expr => AND(expr, parseExpression(tok)))
          case AlphaToken(opr, _) if opr.equalsIgnoreCase("OR") =>
            expression = expression.map(expr => OR(expr, parseExpression(tok)))
          case token =>
            throw new SyntaxException(s"Invalid start of expression", token)
        }
      }

      else {
        expression = Option(parseExpression(tok))
      }
    }
    expression
  }

  private def parseExpression(tok: Iterator[Token]): Expression = {
    tok.take(3).toList match {
      case List(AlphaToken(name, _), opt@OperatorToken(operator, _), token) =>
        operator match {
          case "=" => EQ(Field(name), Evaluatable(token.value))
          case ">" => GT(Field(name), Evaluatable(token.value))
          case ">=" => GE(Field(name), Evaluatable(token.value))
          case "<" => LT(Field(name), Evaluatable(token.value))
          case "<=" => LE(Field(name), Evaluatable(token.value))
          case "!=" => NE(Field(name), Evaluatable(token.value))
          case "<>" => NE(Field(name), Evaluatable(token.value))
          case _ =>
            throw new SyntaxException(s"Invalid operator", token)
        }
      case List(ft, opt@AlphaToken(operator, _), QuotedToken(matching, _)) =>
        operator match {
          case "LIKE" => LIKE(Field(ft.text), matching)
          case _ =>
            throw new SyntaxException(s"Invalid operator", opt)
        }
      case tokens =>
        throw new SyntaxException(s"Expected expression", tokens.headOption.orNull)
    }
  }

  /**
    * Parses a field list (e.g. "Symbol", ",", "Name", ",", "Sector", ",", "Industry", ",", "LastSale", ",", "MarketCap")
    * @param args the arguments list
    * @return a list of [[Field fields]]
    */
  private def parseFields(args: Seq[Token]): List[Field] = {
    val it = args.toIterator
    var fields: List[Field] = Nil
    while (it.hasNext) {
      val args = it.takeWhile(_.text != ",")
      fields = args.toList.map(t => Field(t.text)) ::: fields
    }
    fields.reverse
  }

  /**
    * Parses a limit expression
    * @param args the given argument list
    * @return the optional count
    */
  private def parseLimit(args: Seq[Token]): Option[Int] = {
    args match {
      case Seq(nt: NumericToken) => Some(nt.value.toInt)
      case tokens => throw new SyntaxException("Numeric value expected", tokens.headOption.orNull)
    }
  }

  private def parseSource(expr: Seq[Token]): Option[QueryInputSource] = {
    expr.headOption.map(t => DelimitedInputSource(t.text))
  }

  private def parseTarget(expr: Seq[Token]): QueryOutputSource = {
    expr.headOption.map(t => DelimitedOutputSource(t.text))
      .getOrElse(throw new SyntaxException("Target expression expected", expr.headOption.orNull))
  }

}

/**
  * Query Compiler Companion
  * @author lawrence.daniels@gmail.com
  */
object QueryCompiler {

  /**
    * LinkedHashMap Extensions
    * @param map the given [[mutable.LinkedHashMap map]]
    */
  final implicit class LinkedHashMapExtensions[T](val map: mutable.LinkedHashMap[String, T]) extends AnyVal {

    @inline
    def containsIgnoreCase(key: String): Boolean = map.keys.exists(_.equalsIgnoreCase(key))

    @inline
    def getIgnoreCase(key: String): Option[T] = map.find(_._1.equalsIgnoreCase(key)).map(_._2)

  }

  /**
    * Map Extensions
    * @param map the given [[Map map]]
    */
  final implicit class MapExtensions[T](val map: Map[String, T]) extends AnyVal {

    @inline
    def containsIgnoreCase(key: String): Boolean = map.keys.exists(_.equalsIgnoreCase(key))

    @inline
    def getIgnoreCase(key: String): Option[T] = map.find(_._1.equalsIgnoreCase(key)).map(_._2)

  }

}