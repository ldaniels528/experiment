package com.github.ldaniels528.transgress.server
package routes

import com.github.ldaniels528.transgress.server.dao.TriggerDAO._
import com.github.ldaniels528.transgress.LoggerFactory
import io.scalajs.npm.express.Application
import io.scalajs.npm.expressws.WsRouting
import io.scalajs.npm.mongodb.Db

import scala.concurrent.ExecutionContext

/**
  * Trigger Routes
  * @author lawrence.daniels@gmail.com
  */
class TriggerRoutes(app: Application with WsRouting, db: Db)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val triggerDAO = db.getTriggerDAO

}
