package com.github.ldaniels528.broadway.watcher

/**
  * Represents a trigger constraint
  * @author lawrence.daniels@gmail.com
  */
trait Constraint {
  def matches(name: String): Boolean
}

/**
  * Contains constraint
  * @param substring  the substring to compare
  * @param ignoreCase indicate whether the case should be ignored
  */
case class ContainsConstraint(substring: String, ignoreCase: Boolean = false) extends Constraint {
  private val _substring = substring.toLowerCase()

  override def matches(name: String): Boolean = {
    if (ignoreCase) name.toLowerCase.contains(_substring) else name.contains(substring)
  }
}

/**
  * Equals constraint
  * @param string  the string to compare
  * @param ignoreCase indicate whether the case should be ignored
  */
case class EqualsConstraint(string: String, ignoreCase: Boolean = false) extends Constraint {
  override def matches(name: String): Boolean = if (ignoreCase) name.equalsIgnoreCase(string) else name == string
}

/**
  * Prefix constraint
  * @param prefix  the string to compare
  * @param ignoreCase indicate whether the case should be ignored
  */
case class PrefixConstraint(prefix: String, ignoreCase: Boolean = false) extends Constraint {
  private val _prefix = prefix.toLowerCase()

  override def matches(name: String): Boolean = {
    if (ignoreCase) name.toLowerCase.startsWith(_prefix) else name.startsWith(prefix)
  }
}

/**
  * RegEx constraint
  * @param pattern  the pattern to compare
  */
case class RegExConstraint(pattern: String) extends Constraint {
  override def matches(name: String): Boolean = name.matches(pattern)
}

/**
  * Suffix constraint
  * @param suffix  the string to compare
  * @param ignoreCase indicate whether the case should be ignored
  */
case class SuffixConstraint(suffix: String, ignoreCase: Boolean = false) extends Constraint {
  private val _suffix = suffix.toLowerCase()

  override def matches(name: String): Boolean = {
    if (ignoreCase) name.toLowerCase.endsWith(_suffix) else name.endsWith(suffix)
  }
}
