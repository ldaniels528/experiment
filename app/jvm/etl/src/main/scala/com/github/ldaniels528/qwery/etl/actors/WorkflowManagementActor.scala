package com.github.ldaniels528.qwery.etl.actors

import java.io.File

import akka.actor.{Actor, ActorLogging}
import com.github.ldaniels528.qwery.actors.ActorSupport
import com.github.ldaniels528.qwery.devices.DeviceHelper._
import com.github.ldaniels528.qwery.etl.ETLConfig
import com.github.ldaniels528.qwery.etl.actors.WorkflowManagementActor._
import com.github.ldaniels528.qwery.etl.triggers.Trigger
import com.github.ldaniels528.qwery.ops.{ResultSet, Scope, Variable}

/**
  * Workflow Management Actor
  * @author lawrence.daniels@gmail.com
  */
class WorkflowManagementActor(config: ETLConfig) extends Actor with ActorLogging with ActorSupport {
  private implicit val dispatcher = context.dispatcher

  override def receive: Receive = {
    case ProcessFile(file, trigger, scope) => sender ! processFile(file, trigger, scope)
    case message =>
      log.warning(s"Unexpected message '$message' (${Option(message).map(_.getClass.getName).orNull})")
      unhandled(message)
  }

  private def processFile(workFile: File, trigger: Trigger, rootScope: Scope): ResultSet = {
    // set a new scope with processing variables
    val scope = Scope(rootScope)
    scope += Variable("work.file.base", Option(workFile.getBaseName))
    scope += Variable("work.file.name", Option(workFile.getName))
    scope += Variable("work.file.path", Option(workFile.getCanonicalPath))
    scope += Variable("work.file.size", Option(workFile).map(_.getSize))
    scope += Variable("work.path", Option(workFile.getParentFile))

    // start the workflow
    trigger.execute(scope, workFile.getName)
  }

}

/**
  * Workflow Management Actor Companion
  * @author lawrence.daniels@gmail.com
  */
object WorkflowManagementActor {

  case class ProcessFile(file: File, trigger: Trigger, scope: Scope)

}
