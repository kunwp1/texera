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

package org.apache.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.WorkflowContext
import org.apache.texera.amber.engine.architecture.coordinator.{CoordinatorConfig, Workflow}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.offload.RuntimeOffloadOrchestrator
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmptyRequest
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import org.apache.texera.amber.engine.common.Utils
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.engine.common.executionruntimestate.ExecutionMetadataStore
import org.apache.texera.web.model.websocket.event.{
  TexeraWebSocketEvent,
  WorkflowErrorEvent,
  WorkflowStateEvent
}
import org.apache.texera.web.model.websocket.request.WorkflowExecuteRequest
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource
import org.apache.texera.web.storage.ExecutionStateStore
import org.apache.texera.web.storage.ExecutionStateStore.updateWorkflowState
import org.apache.texera.web.{ComputingUnitMaster, SubscriptionManager, WebsocketInput}
import org.apache.texera.common.compiler.{CompilationErrorHandling, WorkflowCompiler}

import java.net.URI
import scala.collection.mutable

object WorkflowExecutionService {
  def getLatestExecutionId(
      workflowId: WorkflowIdentity,
      computingUnitId: Int
  ): Option[ExecutionIdentity] = {
    WorkflowExecutionsResource
      .getLatestExecutionID(workflowId.id.toInt, computingUnitId)
      .map(eid => new ExecutionIdentity(eid.longValue()))
  }
}

class WorkflowExecutionService(
    coordinatorConfig: CoordinatorConfig,
    val workflowContext: WorkflowContext,
    resultService: ExecutionResultService,
    request: WorkflowExecuteRequest,
    val executionStateStore: ExecutionStateStore,
    errorHandler: Throwable => Unit,
    userEmailOpt: Option[String],
    sessionUri: URI
) extends SubscriptionManager
    with LazyLogging {

  // Wire error/state reporting first, before any other construction work, so a
  // fatalErrors update (recorded by errorHandler) always has an emitter.
  // Construction itself does no external work and cannot throw; the throwing
  // work lives in executeWorkflow(), whose failures reach the UI through this
  // same handler.
  addSubscription(
    executionStateStore.metadataStore.registerDiffHandler((oldState, newState) => {
      val outputEvents = new mutable.ArrayBuffer[TexeraWebSocketEvent]()

      if (newState.state != oldState.state || newState.isRecovering != oldState.isRecovering) {
        outputEvents.append(createStateEvent(newState))
        // Release rented offload instances as soon as the execution reaches a
        // terminal state. Waiting for unsubscribeAll() would keep them billing
        // until the browser session ends, long after the work is done.
        // Asynchronously: this handler runs under the metadata store's lock.
        if (isTerminalState(newState.state)) {
          releaseOffloadInstances(async = true)
        }
      }

      if (newState.fatalErrors != oldState.fatalErrors) {
        outputEvents.append(WorkflowErrorEvent(newState.fatalErrors))
      }

      outputEvents
    })
  )

  // Must match the terminal set used elsewhere (ExecutionResultService,
  // SyncExecutionResource): TERMINATED is reachable via controller-side teardown,
  // and omitting it would leave rented offload instances billing until the
  // browser session ends.
  private def isTerminalState(state: WorkflowAggregatedState): Boolean =
    state == COMPLETED || state == FAILED || state == KILLED || state == TERMINATED

  workflowContext.workflowSettings = request.workflowSettings
  val wsInput = new WebsocketInput(errorHandler)

  private def createStateEvent(state: ExecutionMetadataStore): WorkflowStateEvent = {
    if (state.isRecovering && state.state != COMPLETED) {
      WorkflowStateEvent("Recovering")
    } else {
      WorkflowStateEvent(Utils.aggregatedStateToString(state.state))
    }
  }

  var workflow: Workflow = _

  // Runtime starts from here:
  logger.info("Initialing an AmberClient, runtime starting...")
  var client: AmberClient = _
  var executionReconfigurationService: ExecutionReconfigurationService = _
  var executionStatsService: ExecutionStatsService = _
  var executionRuntimeService: ExecutionRuntimeService = _
  var executionConsoleService: ExecutionConsoleService = _

  // Holds rented instances for offloaded operators between prepare() and
  // release(); None when nothing is offloaded.
  //
  // Volatile because the write happens on the request thread (executeWorkflow)
  // while the reads happen on whichever thread publishes a terminal state into
  // the metadata store, and on the websocket thread during unsubscribeAll.
  // Without it, a reader can miss the Some(...) write and skip the release,
  // leaving rented instances running.
  @volatile private var offloadOrchestrator: Option[RuntimeOffloadOrchestrator] = None

  def executeWorkflow(): Unit = {
    // Kept outside the try so the error path can release the rentals even if the
    // terminal-state handler concurrently took the orchestrator off the field
    // (possible while prepare is still renting, which can take minutes).
    var preparedOrchestrator: Option[RuntimeOffloadOrchestrator] = None
    try {
      val compilationResult = new WorkflowCompiler(workflowContext)
        .compile(request.logicalPlan, CompilationErrorHandling.Strict)
      workflow = Workflow.fromCompilationResult(workflowContext, compilationResult)

      // Per-operator offloading: if any operator opts in, rent an instance for
      // each and pin it there. Done after compilation (so validation errors are
      // already surfaced) and before the runtime is created (which consumes the
      // physical plan). A failure here aborts the run rather than silently
      // executing un-offloaded, which would defeat the operator's memory sizing.
      //
      // The pinned plan replaces workflow.physicalPlan rather than living beside
      // it, so every downstream reader (runtime, result service, reconfiguration)
      // sees the one plan that is actually executing.
      if (RuntimeOffloadOrchestrator.isNeeded(workflow.logicalPlan.operators)) {
        val orchestrator = RuntimeOffloadOrchestrator.forExecution(
          AmberRuntime.actorSystem,
          workflowContext.executionId.id
        )
        preparedOrchestrator = Some(orchestrator)
        offloadOrchestrator = preparedOrchestrator
        workflow = workflow.copy(
          physicalPlan = orchestrator.prepare(workflow.logicalPlan.operators, workflow.physicalPlan)
        )
      }
    } catch {
      case err: Throwable =>
        // stop here: `workflow` may be null or instances may be rented; release
        // any and report, so falling through does not NPE or leak instances.
        releaseOffloadInstances()
        // release() is idempotent, so this is a no-op when the line above already
        // released. It covers the case where the field was cleared by a
        // concurrent terminal-state release that ran before prepare recorded its
        // rentals -- without it, those instances would have no owner.
        preparedOrchestrator.foreach(_.release())
        errorHandler(err)
        return
    }

    client = ComputingUnitMaster.createAmberRuntime(
      workflow.context,
      workflow.physicalPlan,
      coordinatorConfig,
      errorHandler
    )
    executionReconfigurationService =
      new ExecutionReconfigurationService(client, executionStateStore, workflow)
    executionStatsService = new ExecutionStatsService(client, executionStateStore, workflow.context)
    executionRuntimeService = new ExecutionRuntimeService(
      client,
      executionStateStore,
      wsInput,
      executionReconfigurationService,
      coordinatorConfig.faultToleranceConfOpt,
      workflowContext.workflowId.id,
      request.emailNotificationEnabled,
      userEmailOpt,
      sessionUri
    )
    executionConsoleService =
      new ExecutionConsoleService(client, executionStateStore, wsInput, workflow.context)

    logger.info("Starting the workflow execution.")
    resultService.attachToExecution(
      workflow.context.executionId,
      executionStateStore,
      workflow.physicalPlan,
      client
    )
    executionStateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(READY, metadataStore)
        .withFatalErrors(Seq.empty)
    )
    executionStateStore.statsStore.updateState(stats =>
      stats.withStartTimeStamp(System.currentTimeMillis())
    )
    client.coordinatorInterface
      .startWorkflow(EmptyRequest(), ())
      .onFailure(err => {
        errorHandler(err)
      })
      .onSuccess(resp =>
        executionStateStore.metadataStore.updateState(metadataStore =>
          if (metadataStore.state != FAILED) {
            updateWorkflowState(resp.workflowState, metadataStore)
          } else {
            metadataStore
          }
        )
      )
  }

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    if (client != null) {
      // runtime created
      client.shutdown()
      executionRuntimeService.unsubscribeAll()
      executionConsoleService.unsubscribeAll()
      executionStatsService.unsubscribeAll()
      executionReconfigurationService.unsubscribeAll()
    }
    // Backstop: normally the terminal-state handler already released these, but
    // an execution that never reached a terminal state must not leave instances
    // billing after the session ends.
    releaseOffloadInstances()
  }

  /**
    * Releases rented offload instances, if any. Safe to call repeatedly and from
    * teardown: [[RuntimeOffloadOrchestrator.release]] already swallows per-instance
    * failures, so nothing here can throw.
    *
    * @param async run the release on another thread. Required when called from a
    *              metadata-store diff handler: that runs under the store's lock,
    *              and releasing spawns a `docker rm` subprocess per container with
    *              no timeout. A wedged daemon would otherwise hold the lock
    *              forever, blocking every later state update for this execution --
    *              and, when the publisher is the ClusterListener actor, wedging
    *              the actor that answers cluster-membership asks for the whole JVM.
    */
  private def releaseOffloadInstances(async: Boolean = false): Unit = {
    // Synchronized so the take-and-clear is atomic: the terminal-state handler and
    // unsubscribeAll() can run on different threads, and a plain check-then-clear
    // would let both observe the same orchestrator and release twice.
    val toRelease = synchronized {
      val current = offloadOrchestrator
      offloadOrchestrator = None
      current
    }
    toRelease.foreach { orchestrator =>
      if (async) {
        val thread = new Thread(
          () => orchestrator.release(),
          s"offload-release-${workflowContext.executionId.id}"
        )
        thread.setDaemon(true)
        thread.start()
      } else {
        orchestrator.release()
      }
    }
  }

}
