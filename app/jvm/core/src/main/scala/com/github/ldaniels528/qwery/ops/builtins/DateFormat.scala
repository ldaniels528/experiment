package com.github.ldaniels528.qwery.ops.builtins

import java.text.SimpleDateFormat

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Date Format function
  * @param dateValue    the given date value
  * @param formatString the given date format (e.g. "yyyy-MM-dd")
  */
case class DateFormat(dateValue: Expression, formatString: Expression) extends Expression {

  override def evaluate(scope: Scope): Option[Any] = {
    for {
      date <- dateValue.getAsDate(scope)
      format <- formatString.getAsString(scope)
    } yield new SimpleDateFormat(format).format(date)
  }

}
