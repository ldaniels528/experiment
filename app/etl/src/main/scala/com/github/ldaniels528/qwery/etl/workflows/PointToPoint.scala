package com.github.ldaniels528.qwery.etl.workflows

import java.util.UUID

import akka.actor.ActorRef
import com.github.ldaniels528.qwery.actors.QweryActorSystem
import com.github.ldaniels528.qwery.etl.actors.ResourceReadingActor
import com.github.ldaniels528.qwery.etl.actors.ResourceReadingActor.ReadFile
import com.github.ldaniels528.qwery.ops.{RootScope, Row, Scope}
import com.github.ldaniels528.qwery.sources.{DataResource, OutputSource, Statistics}

import scala.concurrent.{Future, Promise}

/**
  * Represents a Point-to-Point (single input, single output) workflow
  * @param pid    the unique process ID of the workflow
  * @param source the workflow's input resource
  * @param target the workflow's output resource
  */
case class PointToPoint(pid: UUID, source: DataResource, target: DataResource, scope: Scope) extends Workflow {
  private var output: Option[OutputSource] = None
  private val promise = Promise[Option[Statistics]]()

  override def close(statistics: Option[Statistics]): Unit = {
    output.foreach(_.close())
    promise.success(statistics)
  }

  override def start(actor: ActorRef): Future[Option[Statistics]] = {
    // open the output source
    output = Option {
      val out = target.getOutputSource(scope, append = false)
        .getOrElse(throw new IllegalArgumentException(s"[$pid] No output device found for '$target'"))
      out.open(RootScope())
      out
    }

    // start read from the input source
    val reader = QweryActorSystem.createActor[ResourceReadingActor]
    reader ! ReadFile(pid, source, scope, recipient = actor)
    promise.future
  }

  override def write(row: Row): Unit = output.foreach(_.write(row))

}