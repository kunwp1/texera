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

package org.apache.texera.amber.clustering

import org.apache.pekko.actor.{Actor, ActorRefFactory, Address}
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.apache.pekko.cluster.ClusterEvent._
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import com.google.protobuf.timestamp.Timestamp
import com.twitter.util.{Await, Future}
import org.apache.texera.amber.clustering.ClusterListener.numWorkerNodesInCluster
import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  FAILED
}
import org.apache.texera.amber.engine.common.{AmberLogging, AmberRuntime}
import org.apache.texera.amber.error.ErrorUtils.getStackTraceWithAllCauses
import org.apache.texera.web.SessionState
import org.apache.texera.web.model.websocket.response.ClusterStatusUpdateEvent
import org.apache.texera.web.service.{WorkflowExecutionService, WorkflowService}
import org.apache.texera.web.storage.ExecutionStateStore.updateWorkflowState

import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object ClusterListener {

  /** Every cluster member, including nodes dedicated to one offloaded operator. */
  final case class GetAvailableNodeAddresses()

  /**
    * Members eligible for general round-robin placement, i.e. excluding nodes
    * rented for a single offloaded operator.
    */
  final case class GetGeneralPlacementAddresses()

  /**
    * The members the coordinator may hand work to: full members only.
    *
    * `cluster.state.members` reports every member whatever its status, and publishing
    * that unfiltered is what let a rented offload node be used while still `Joining`.
    * With `leader-actions-interval = 10s` the Joining window is ~8s wide -- long enough
    * for an operator to be deployed, run, and torn down inside it -- and remote death
    * watch delivers no `Terminated` for an actor on a member below `Up` (characterised
    * in `RemoteGracefulStopSpec`). Region teardown's `gracefulStop` therefore timed out
    * on workers that had stopped cleanly, wedging the execution and stranding the
    * rental.
    *
    * `WeaklyUp` is excluded with the rest: it exists precisely because the leader could
    * not act, so it carries no better guarantee than `Joining`.
    */
  private[clustering] def placeableAddresses(
      members: Iterable[(Address, MemberStatus)]
  ): Iterable[Address] =
    members.collect { case (address, MemberStatus.Up) => address }

  var numWorkerNodesInCluster = 0

  /** Path the listener is registered at; see AmberRuntime.createAmberSystem. */
  private val ActorPath = "/user/cluster-info"

  private val AskTimeout: FiniteDuration = 5.seconds

  /**
    * Addresses whose departure from the cluster is expected.
    *
    * A `MemberRemoved` event normally means a node failed, and the handler
    * force-stops every non-completed execution in response. Per-operator
    * offloading makes a node leaving a *routine* event: releasing a rented
    * instance deliberately removes it. Without this registry, tearing down one
    * finished workflow's container would force-fail unrelated executions that are
    * merely running, and would overwrite an already-KILLED execution with a
    * spurious FAILED.
    *
    * Concurrent map (address -> when announced): written by the release path,
    * read by the listener actor.
    */
  private val expectedDepartures =
    new java.util.concurrent.ConcurrentHashMap[String, java.lang.Long]()

  /**
    * How long an announced departure stays valid.
    *
    * An announcement is normally consumed within seconds, when the cluster
    * notices the node is gone. Expiring stale entries keeps the map from growing
    * without bound in a long-lived coordinator when the matching MemberRemoved
    * never arrives -- e.g. the container was already killed externally. It also
    * means a much-later genuine failure at a reused address is still handled as a
    * failure rather than silently ignored.
    */
  private val DepartureExpiry: FiniteDuration = 10.minutes

  /** Records that `nodeAddress` is being released on purpose. */
  def expectDeparture(nodeAddress: String): Unit = {
    val now = System.nanoTime()
    expectedDepartures.put(nodeAddress, now)
    // Opportunistic sweep: no timer thread, and the map holds at most one entry
    // per concurrently-releasing instance in practice.
    expectedDepartures
      .entrySet()
      .removeIf(e => now - e.getValue > DepartureExpiry.toNanos)
  }

  /**
    * Whether `address` left because we released it, clearing the expectation.
    *
    * Consuming the entry means a node that is released, then later genuinely
    * fails at the same address, is still treated as a failure.
    */
  def consumeExpectedDeparture(address: Address): Boolean = {
    val announcedAt = expectedDepartures.remove(address.toString)
    announcedAt != null && System.nanoTime() - announcedAt <= DepartureExpiry.toNanos
  }

  /** Announcements still awaiting a matching departure; for tests and diagnostics. */
  private[clustering] def pendingDepartureCount: Int = expectedDepartures.size()

  /**
    * Current cluster member addresses, asked of the listener actor.
    *
    * Callers that are actors should prefer PekkoActorService.getClusterNodeAddresses,
    * which is bound to an ActorContext. This overload exists for callers outside
    * an actor (e.g. offload provisioning from the execution service), so the
    * actor path and timeout are not restated at each call site.
    */
  def availableNodeAddresses(actorRefFactory: ActorRefFactory): Array[Address] =
    askForAddresses(actorRefFactory, GetAvailableNodeAddresses())

  /**
    * Members eligible for general round-robin placement, excluding nodes rented
    * for a single offloaded operator.
    */
  def generalPlacementAddresses(actorRefFactory: ActorRefFactory): Array[Address] =
    askForAddresses(actorRefFactory, GetGeneralPlacementAddresses())

  private def askForAddresses(actorRefFactory: ActorRefFactory, message: Any): Array[Address] = {
    implicit val timeout: Timeout = AskTimeout
    scala.concurrent.Await.result(
      actorRefFactory.actorSelection(ActorPath).ask(message),
      AskTimeout
    ) match {
      case addresses: Array[Address] => addresses
      case other =>
        throw new IllegalStateException(
          s"Expected node addresses from $ActorPath, got: ${other.getClass.getName}"
        )
    }
  }
}

class ClusterListener extends Actor with AmberLogging {

  val actorId: ActorVirtualIdentity = ActorVirtualIdentity("ClusterListener")
  val cluster: Cluster = Cluster(context.system)

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(
      self,
      initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent]
    )
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case evt: MemberEvent =>
      logger.info(s"received member event = $evt")
      updateClusterStatus(evt)
    case ClusterListener.GetAvailableNodeAddresses() =>
      sender() ! getAllAddress.toArray
    case ClusterListener.GetGeneralPlacementAddresses() =>
      sender() ! getGeneralPlacementAddresses.toArray
    case other =>
      println(other)
  }

  private def getAllAddress: Iterable[Address] = {
    ClusterListener.placeableAddresses(cluster.state.members.toSeq.map(m => (m.address, m.status)))
  }

  /**
    * Addresses eligible for general (round-robin) worker placement.
    *
    * Excludes nodes rented for a single offloaded operator: each is sized for
    * exactly one operator, so letting round-robin put another operator's workers
    * there would invalidate that sizing and could get the offloaded operator
    * OOM-killed by a co-tenant. Pinned placement targets such a node by address
    * and reads membership without this role filter, so it is unaffected -- though
    * both views are restricted to `Up` members by `placeableAddresses`.
    */
  private def getGeneralPlacementAddresses: Iterable[Address] = {
    ClusterListener.placeableAddresses(
      cluster.state.members.toSeq
        .filterNot(_.hasRole(AmberRuntime.DedicatedOffloadRole))
        .map(m => (m.address, m.status))
    )
  }

  private def forcefullyStop(executionService: WorkflowExecutionService, cause: Throwable): Unit = {
    executionService.client.shutdown()
    executionService.executionStateStore.statsStore.updateState(stats =>
      stats.withEndTimeStamp(System.currentTimeMillis())
    )
    executionService.executionStateStore.metadataStore.updateState { metadataStore =>
      logger.error("forcefully stopping execution", cause)
      updateWorkflowState(FAILED, metadataStore).addFatalErrors(
        WorkflowFatalError(
          EXECUTION_FAILURE,
          Timestamp(Instant.now),
          cause.toString,
          getStackTraceWithAllCauses(cause),
          "unknown operator"
        )
      )
    }
  }

  private def updateClusterStatus(evt: MemberEvent): Unit = {
    evt match {
      case MemberRemoved(member, _) if ClusterListener.consumeExpectedDeparture(member.address) =>
        // A rented offload instance we released on purpose. Falling through to the
        // failure handling below would force-stop every non-completed execution --
        // including unrelated ones that are merely running, and an already-KILLED
        // one, which would be overwritten with a spurious FAILED.
        logger.info(s"Cluster node ${member.address} left as expected (offload release)")

      case MemberRemoved(member, status) =>
        logger.info("Cluster node " + member + " is down!")
        val futures = new ArrayBuffer[Future[_]]
        WorkflowService.getAllWorkflowServices.foreach { workflow =>
          val executionService = workflow.executionService.getValue
          if (
            executionService != null && executionService.executionStateStore.metadataStore.getState.state != COMPLETED
          ) {
            if (ApplicationConfig.isFaultToleranceEnabled) {
              logger.info(
                s"Trigger recovery process for execution id = ${executionService.executionStateStore.metadataStore.getState.executionId.id}"
              )
              try {
                futures.append(executionService.client.notifyNodeFailure(member.address))
              } catch {
                case t: Throwable =>
                  logger.warn(
                    s"execution ${executionService.workflowContext.executionId.id} cannot recover! forcing it to stop"
                  )
                  forcefullyStop(executionService, t)
              }
            } else {
              logger.info(
                s"Kill execution id = ${executionService.executionStateStore.metadataStore.getState.executionId.id}"
              )
              forcefullyStop(
                executionService,
                new RuntimeException("fault tolerance is not enabled")
              )
            }
          }
        }
        Await.all(futures.toSeq: _*)
      case other => //skip
    }

    // Counts the general pool, not raw membership: a node rented for one
    // offloaded operator is not capacity anyone else can use, so reporting it in
    // the UI's cluster-size indicator would overstate available compute and make
    // the number jump around as offloaded operators come and go.
    numWorkerNodesInCluster = getGeneralPlacementAddresses.size
    SessionState.getAllSessionStates.foreach { state =>
      state.send(ClusterStatusUpdateEvent(numWorkerNodesInCluster))
    }

    logger.info(
      "---------Now we have " + numWorkerNodesInCluster + s" nodes in the cluster---------"
    )

  }

}
