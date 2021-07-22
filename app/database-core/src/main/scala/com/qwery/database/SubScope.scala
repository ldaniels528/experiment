package com.qwery.database

import com.qwery.database.models.KeyValues
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Creates a new child Scope
 * @param parent the parent [[Scope scope]]
 */
class SubScope(parent: Scope) extends Scope {
  private val child = Scope()

  override def currentRow: Option[KeyValues] = child.currentRow ?? parent.currentRow

  override def currentRow_=(row: Option[KeyValues]): Unit = child.currentRow = row

  override def get(name: String): Option[Any] = child.get(name) ?? parent.get(name)

  override def addVariable(variable: Scope.Variable): Unit = child.addVariable(variable)

  override def getVariable(name: String): Option[Scope.Variable] = child.getVariable(name) ?? parent.getVariable(name)

}