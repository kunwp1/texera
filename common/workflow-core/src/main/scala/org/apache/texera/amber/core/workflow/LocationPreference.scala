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

package org.apache.texera.amber.core.workflow

// LocationPreference defines where operators should run.
sealed trait LocationPreference extends Serializable

// PreferCoordinator: Run on the coordinator node.
// Example: For scan operators reading files.
object PreferCoordinator extends LocationPreference

// RoundRobinPreference: Distribute across worker nodes, per operator.
// Example:
// - Operator A: Worker 1 -> Node 1, Worker 2 -> Node 2, Worker 3 -> Node 3
// - Operator B: Worker 1 -> Node 1, Worker 2 -> Node 2
object RoundRobinPreference extends LocationPreference

/**
  * PreferPinnedAddress: Run every worker of this operator on one specific
  * cluster node, identified by its Pekko address.
  *
  * Unlike the cluster-wide preferences above, this pins a single operator to a
  * single node. It backs per-operator offloading: a node is provisioned for one
  * operator (e.g. a rented VM sized for that operator's peak memory), joins the
  * cluster, and the operator is pinned to it.
  *
  * The address must name a node that is a current cluster member at deployment
  * time. Deployment fails rather than falling back to another node, because a
  * silent fallback would place the operator on hardware that was not sized for
  * it -- exactly the out-of-memory failure the offload was meant to avoid.
  *
  * @param nodeAddress the target node's Pekko address, e.g.
  *                    "pekko://Amber@10.0.1.5:2552"
  */
case class PreferPinnedAddress(nodeAddress: String) extends LocationPreference {
  require(
    nodeAddress != null && nodeAddress.trim.nonEmpty,
    "PreferPinnedAddress requires a non-blank node address"
  )
}
