package com.qwery.platform.sparksql.generator

import com.qwery.platform.sparksql.generator.CodeBuilder._

/**
  * Code Builder
  * @author lawrence.daniels@gmail.com
  */
case class CodeBuilder(indent: Int = 2, prepend: String = "") {
  private var segments: List[Segment] = Nil

  def append(cb: CodeBuilder): this.type = {
    val newSegments = cb.segments match {
      case Nil => Nil
      case head :: tail => head.copy(prepend = "", indent = cb.indent + indent) :: tail.map(_.copy(indent = cb.indent + indent))
    }
    append(buildCode(newSegments, noCRLF = true))
  }

  def append(lines: Iterable[String]): this.type = {
    segments = segments ::: lines.map(text => Segment(text, indent, prepend)).toList
    this
  }

  def append(lineOpt: Option[String]): this.type = {
    lineOpt.foreach(line => segments = segments ::: Segment(line, indent, prepend) :: Nil)
    this
  }

  def append(lines: String*): this.type = {
    segments = segments ::: lines.map(text => Segment(text, indent, prepend)).toList
    this
  }

  def build(noCRLF: Boolean = false): String = buildCode(segments, noCRLF)

  override def toString: String = build()

}

/**
  * Code Builder Companion
  * @author lawrence.daniels@gmail.com
  */
object CodeBuilder {

  /**
    * Generates code via the given template
    * @param templateString the given template string
    * @param props          the given properties
    * @return the generated code
    */
  def template(templateString: String)(props: (String, String)*): String = {
    import com.qwery.util.StringHelper._

    // prepare the contents of the template
    val code = new StringBuilder(templateString)
    val mapping = Map(props: _*)
    val codeBegin = "{{"
    val codeEnd = "}}"
    var lastIndex = 0
    var done = false

    // replace the "{{property}}" tags
    while (!done) {
      val results = for {
        start <- code.indexOfOpt(codeBegin, lastIndex)
        end <- code.indexOfOpt(codeEnd, start).map(_ + codeEnd.length)
        property = code.substring(start + codeBegin.length, end - codeEnd.length).trim
      } yield {
        // determine the replacement value
        val replacement = mapping.getOrElse(property, throw new IllegalArgumentException(s"Property '$property' is required"))

        // replace the tag
        code.replace(start, end, replacement)
        lastIndex = start + replacement.length
      }
      done = results.isEmpty
    }
    code.toString()
  }

  private def buildCode(segments: List[Segment], noCRLF: Boolean): String = {
    segments.filterNot(_.isEmpty) match {
      case Nil => ""
      case first :: remaining =>
        val sb = new StringBuilder(first.text)
        remaining foreach { segment =>
          sb.append('\n')
            .append(" " * segment.indent)
            .append(segment.prepend)
            .append(segment.text)
        }
        if (!noCRLF) sb.append('\n')
        sb.toString()
    }
  }

  /**
    * Represents a code segment
    * @param text    the line of text the segment represents
    * @param indent  indicates the number of spaces to indent
    * @param prepend optional, prepend value for every segment except the first one
    */
  case class Segment(text: String, indent: Int = 2, prepend: String = "") {

    def isEmpty: Boolean = text.isEmpty

    def nonEmpty: Boolean = text.nonEmpty

    override def toString: String = text
  }

}