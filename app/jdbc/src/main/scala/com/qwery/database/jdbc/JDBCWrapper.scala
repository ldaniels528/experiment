package com.qwery.database
package jdbc

import java.sql.Wrapper

trait JDBCWrapper extends Wrapper {

  override final def unwrap[T](iface: Class[T]): T = ???

  override final def isWrapperFor(iface: Class[_]): Boolean = false

}
