package com.qwery.models

import com.qwery.models.expressions.RowSetVariableRef

/**
  * FOR-LOOP statement
  * @param variable  the given [[RowSetVariableRef variable]]
  * @param rows      the given [[Invokable rows]]
  * @param invokable the [[Invokable statements]] to execute
  * @param isReverse indicates reverse order
  * @example
  * {{{
  * FOR @item IN REVERSE (SELECT symbol, lastSale FROM Securities WHERE naics = '12345')
  * LOOP
  *   PRINT '${item.symbol} is ${item.lastSale)/share';
  * END LOOP;
  * }}}
  */
case class ForLoop(variable: RowSetVariableRef,
                   rows: Invokable,
                   invokable: Invokable,
                   isReverse: Boolean) extends Invokable