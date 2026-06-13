/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.map

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.dummy.DummyOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.projection.ProjectionOpDesc
import org.apache.texera.amber.operator.{LogicalOp, StateTransferFunc}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success}

class MapOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(42L)
  private val executionId = ExecutionIdentity(7L)

  /** Minimal concrete MapOpDesc that records getPhysicalOp arguments and returns a sentinel. */
  private class RecordingMapOpDesc(sentinel: PhysicalOp) extends MapOpDesc {
    var capturedWorkflowId: Option[WorkflowIdentity] = None
    var capturedExecutionId: Option[ExecutionIdentity] = None

    override def getPhysicalOp(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity
    ): PhysicalOp = {
      capturedWorkflowId = Some(workflowId)
      capturedExecutionId = Some(executionId)
      sentinel
    }

    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Test Map",
        "Test-only MapOpDesc for unit specs",
        OperatorGroupConstants.UTILITY_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }

  private def sentinelPhysicalOp: PhysicalOp =
    PhysicalOp.oneToOnePhysicalOp(
      workflowId,
      executionId,
      OperatorIdentity("map-op-desc-spec-sentinel"),
      OpExecWithClassName("org.apache.texera.amber.operator.map.MapOpExec")
    )

  private def reconfigure(
      oldOpDesc: LogicalOp,
      newOpDesc: LogicalOp,
      wfId: WorkflowIdentity = workflowId,
      execId: ExecutionIdentity = executionId
  ) =
    new RecordingMapOpDesc(sentinelPhysicalOp).runtimeReconfiguration(
      wfId,
      execId,
      oldOpDesc,
      newOpDesc
    )

  "MapOpDesc.runtimeReconfiguration" should "always succeed with the new desc's PhysicalOp and no state transfer" in {
    val sentinel = sentinelPhysicalOp
    val newDesc = new RecordingMapOpDesc(sentinel)
    val oldDesc = new DummyOpDesc

    val result = reconfigure(oldDesc, newDesc)

    result.isSuccess shouldBe true
    result match {
      case Success((physicalOp, stateTransfer)) =>
        physicalOp shouldBe theSameInstanceAs(sentinel)
        physicalOp shouldBe theSameInstanceAs(newDesc.getPhysicalOp(workflowId, executionId))
        stateTransfer shouldBe None
      case Failure(exception) =>
        fail(s"expected Success, got Failure($exception)")
    }
  }

  it should "ignore the oldOpDesc argument" in {
    val sentinel = sentinelPhysicalOp
    val newDesc = new RecordingMapOpDesc(sentinel)

    val oldDesc1 = new DummyOpDesc
    oldDesc1.dummyOperator = "first-old-desc"

    val oldDesc2 = new ProjectionOpDesc

    val result1 = reconfigure(oldDesc1, newDesc)
    val result2 = reconfigure(oldDesc2, newDesc)

    (result1, result2) match {
      case (Success((physicalOp1, stateTransfer1)), Success((physicalOp2, stateTransfer2))) =>
        physicalOp1 shouldBe theSameInstanceAs(physicalOp2)
        stateTransfer1 shouldBe None
        stateTransfer2 shouldBe None
      case other =>
        fail(s"expected two Success results, got $other")
    }
  }

  it should "propagate workflowId and executionId to newOpDesc.getPhysicalOp" in {
    val distinctWorkflowId = WorkflowIdentity(99L)
    val distinctExecutionId = ExecutionIdentity(123L)
    val sentinel = sentinelPhysicalOp
    val newDesc = new RecordingMapOpDesc(sentinel)
    val oldDesc = new DummyOpDesc

    val result =
      reconfigure(oldDesc, newDesc, distinctWorkflowId, distinctExecutionId)

    result.isSuccess shouldBe true
    newDesc.capturedWorkflowId shouldBe Some(distinctWorkflowId)
    newDesc.capturedExecutionId shouldBe Some(distinctExecutionId)
  }

  it should "not return a state-transfer function for stateless map operators" in {
    val sentinel = sentinelPhysicalOp
    val newDesc = new RecordingMapOpDesc(sentinel)

    reconfigure(new DummyOpDesc, newDesc) match {
      case Success((_, stateTransfer: Option[StateTransferFunc])) =>
        stateTransfer shouldBe None
      case other =>
        fail(s"expected Success with None state transfer, got $other")
    }
  }
}
