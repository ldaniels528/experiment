package com.qwery.models

import com.qwery.models.expressions.ScalarVariableRef

/**
  * FOR-LOOP statement
  * @param variable  the given [[ScalarVariableRef variable]]
  * @param rows      the given [[Invokable rows]]
  * @param invokable the [[Invokable statements]] to execute
  * @param isReverse indicates reverse order
  * @example
  * {{{
  * FOR $item IN REVERSE (SELECT symbol, lastSale FROM Securities WHERE naics = '12345')
  * LOOP
  *   PRINT '${item.symbol} is ${item.lastSale)/share';
  * END LOOP;
  * }}}
  */
case class ForLoop(variable: ScalarVariableRef,
                   rows: Invokable,
                   invokable: Invokable,
                   isReverse: Boolean) extends Invokable