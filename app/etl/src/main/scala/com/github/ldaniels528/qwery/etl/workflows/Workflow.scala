package com.github.ldaniels528.qwery.etl.workflows

import java.util.UUID

import akka.actor.ActorRef
import com.github.ldaniels528.qwery.ops.Row
import com.github.ldaniels528.qwery.sources.Statistics

import scala.concurrent.Future

/**
  * Represents an ETL workflow
  * @author lawrence.daniels@gmail.com
  */
trait Workflow {

  def close(statistics: Option[Statistics]): Unit

  def pid: UUID

  def start(actor: ActorRef): Future[Option[Statistics]]

  def write(row: Row): Unit

}