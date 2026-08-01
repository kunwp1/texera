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

package org.apache.texera.amber.engine.architecture.common

import org.apache.pekko.actor.{Address, Deploy}
import org.apache.pekko.remote.RemoteScope
import org.apache.texera.amber.core.workflow.{
  LocationPreference,
  PhysicalOp,
  PreferCoordinator,
  PreferPinnedAddress,
  RoundRobinPreference
}
import org.apache.texera.amber.engine.architecture.coordinator.execution.OperatorExecution
import org.apache.texera.amber.engine.architecture.deploysemantics.AddressInfo
import org.apache.texera.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import org.apache.texera.amber.engine.architecture.scheduling.config.OperatorConfig
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  FaultToleranceConfig,
  StateRestoreConfig,
  WorkerReplayInitialization
}
import org.apache.texera.amber.util.VirtualIdentityUtils

object ExecutorDeployment {

  /**
    * Resolves the node a single worker should be deployed to.
    *
    * @param locationPreference where the operator wants to run
    * @param addressInfo        current cluster membership
    * @param workerIndex        index of this worker within its operator
    * @return the address to deploy this worker on
    * @throws IllegalStateException if the preference cannot be satisfied
    */
  def resolveAddress(
      locationPreference: LocationPreference,
      addressInfo: AddressInfo,
      workerIndex: Int
  ): Address = {
    locationPreference match {
      case PreferCoordinator =>
        addressInfo.coordinatorAddress

      case RoundRobinPreference =>
        // Dedicated offload nodes are excluded here: each is sized for exactly one
        // operator, so placing another operator's workers there would invalidate
        // that sizing and could OOM-kill the offloaded operator.
        val candidates = addressInfo.generalPlacementAddresses
        if (candidates.isEmpty) {
          throw new IllegalStateException(
            "Execution failed to start, no available computation nodes"
          )
        }
        candidates(workerIndex % candidates.length)

      case PreferPinnedAddress(nodeAddress) =>
        // Deliberately no fallback: an offloaded operator was sized for this
        // specific node, so running it elsewhere risks the very OOM the offload
        // was meant to prevent. Fail loudly instead.
        addressInfo.allAddresses
          .find(_.toString == nodeAddress)
          .getOrElse(
            throw new IllegalStateException(
              s"Execution failed to start: pinned node '$nodeAddress' is not a " +
                s"member of the cluster. Available nodes: " +
                s"[${addressInfo.allAddresses.mkString(", ")}]"
            )
          )
    }
  }

  def createWorkers(
      op: PhysicalOp,
      coordinatorActorService: PekkoActorService,
      operatorExecution: OperatorExecution,
      operatorConfig: OperatorConfig,
      stateRestoreConfig: Option[StateRestoreConfig],
      replayLoggingConfig: Option[FaultToleranceConfig]
  ): Unit = {

    val addressInfo = AddressInfo(
      coordinatorActorService.getClusterNodeAddresses,
      coordinatorActorService.self.path.address,
      Some(coordinatorActorService.getGeneralPlacementNodeAddresses)
    )

    operatorConfig.workerConfigs.foreach(workerConfig => {
      val workerId = workerConfig.workerId
      val workerIndex = VirtualIdentityUtils
        .getWorkerIndex(workerId)
        .getOrElse(
          throw new IllegalStateException(
            s"Expected worker actor id when deploying executor, got: ${workerId.name}"
          )
        )
      val locationPreference = op.locationPreference.getOrElse(RoundRobinPreference)
      val preferredAddress: Address =
        resolveAddress(locationPreference, addressInfo, workerIndex)

      val workflowWorker = if (op.isPythonBased) {
        PythonWorkflowWorker.props(workerConfig)
      } else {
        WorkflowWorker.props(
          workerConfig,
          WorkerReplayInitialization(
            stateRestoreConfig,
            replayLoggingConfig
          )
        )
      }
      // Note: At this point, we don't know if the actor is fully initialized.
      // Thus, the ActorRef returned from `coordinatorActorService.actorOf` is ignored.
      coordinatorActorService.actorOf(
        workflowWorker.withDeploy(Deploy(scope = RemoteScope(preferredAddress)))
      )
      operatorExecution.initWorkerExecution(workerId)
    })
  }

}
