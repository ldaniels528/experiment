package com.github.ldaniels528.qwery.ops.builtins

import java.text.SimpleDateFormat

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Date Parser function
  * @param dateString   the given date string (e.g. "2017-05-11")
  * @param formatString the given date format (e.g. "yyyy-MM-dd")
  */
case class DateParse(dateString: Expression, formatString: Expression) extends Expression {

  override def evaluate(scope: Scope): Option[Any] = {
    for {
      string <- dateString.getAsString(scope)
      format <- formatString.getAsString(scope)
    } yield new SimpleDateFormat(format).parse(string)
  }

}
