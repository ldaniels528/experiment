package com.qwery.database

import com.qwery.database.BinarySearch.OptionComparator

/**
 * Binary Search Capability
 */
trait BinarySearch { device: BlockDevice =>

  /**
   * Performs a binary search of the column for a the specified value
   * @param column the column name
   * @param value  the search value
   * @return an option of a row ID
   */
  def search(column: Symbol, value: Option[Any]): Option[ROWID] = {
    // create a closure to lookup a field value by row ID
    val valueAt: ROWID => Option[Any] = {
      val columnIndex = columns.indexWhere(_.name == column.name)
      (rowID: ROWID) => getField(rowID, columnIndex).value
    }

    // search for a matching field value
    var (p0: ROWID, p1: ROWID, changed: Boolean) = (0, length - 1, true)
    while (p0 != p1 && valueAt(p0) < value && valueAt(p1) > value && changed) {
      val (mp, z0, z1) = ((p0 + p1) / 2, p0, p1)
      if (value >= valueAt(mp)) p0 = mp else p1 = mp
      changed = z0 != p0 || z1 != p1
    }

    // determine whether a match was found
    if (valueAt(p0) == value) Some(p0)
    else if (valueAt(p1) == value) Some(p1)
    else None
  }
  
}

/**
 * Binary Search Companion
 */
object BinarySearch {

  /**
   * Option Enrichment
   * @param optionA the host [[Field field]]
   */
  final implicit class OptionComparator(val optionA: Option[Any]) extends AnyVal {

    def >[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA
        valueB <- optionB if valueA.getClass == valueB.getClass
      } yield {
        (valueA.asInstanceOf[AnyRef], valueB.asInstanceOf[AnyRef]) match {
          case (a: A, b: A) => a.compareTo(b) > 0
          case _ => false
        }
      }).contains(true)
    }

    def >=[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA
        valueB <- optionB if valueA.getClass == valueB.getClass
      } yield {
        (valueA.asInstanceOf[AnyRef], valueB.asInstanceOf[AnyRef]) match {
          case (a: A, b: A) => a.compareTo(b) >= 0
          case _ => false
        }
      }).contains(true)
    }

    def <[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA
        valueB <- optionB if valueA.getClass == valueB.getClass
      } yield {
        (valueA.asInstanceOf[AnyRef], valueB.asInstanceOf[AnyRef]) match {
          case (a: A, b: A) => a.compareTo(b) < 0
          case _ => false
        }
      }).contains(true)
    }

    def <=[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA
        valueB <- optionB if valueA.getClass == valueB.getClass
      } yield {
        (valueA.asInstanceOf[AnyRef], valueB.asInstanceOf[AnyRef]) match {
          case (a: A, b: A) => a.compareTo(b) <= 0
          case _ => false
        }
      }).contains(true)
    }

  }

}
