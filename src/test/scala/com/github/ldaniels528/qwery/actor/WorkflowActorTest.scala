package com.github.ldaniels528.qwery.actor

import com.github.ldaniels528.qwery.actors.WorkflowActor.CopyProcess
import com.github.ldaniels528.qwery.actors.{QweryActorSystem, WorkflowActor}
import org.scalatest.FunSpec

/**
  * Workflow Actor Test
  * @author lawrence.daniels@gmail.com
  */
class WorkflowActorTest extends FunSpec {

  describe("WorkflowActor") {

    it("should process files") {
      val workflow = QweryActorSystem.createActor[WorkflowActor]
      workflow ! CopyProcess(inputPath = "companylist.csv", outputPath = "test4.csv")
      Thread.sleep(6000)
    }

  }

}
