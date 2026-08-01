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

package org.apache.texera.amber.engine.architecture.deploysemantics

import org.apache.pekko.actor.Address

/**
  * Holds worker and coordinator node addresses.
  *
  * @param allAddresses            every cluster member, including nodes rented for
  *                                a single offloaded operator. Pinned placement
  *                                resolves against this, so it must stay complete.
  * @param coordinatorAddress      the coordinator node
  * @param generalPlacementAddresses members eligible for round-robin placement.
  *                                Excludes dedicated offload nodes: each is sized
  *                                for one operator, so a co-tenant's workers would
  *                                invalidate that sizing and could OOM-kill it.
  *                                Defaults to `allAddresses` for callers (e.g.
  *                                tests) with no dedicated nodes.
  */
case class AddressInfo(
    allAddresses: Array[Address],
    coordinatorAddress: Address,
    private val generalPlacementAddressesOpt: Option[Array[Address]] = None
) {
  def generalPlacementAddresses: Array[Address] =
    generalPlacementAddressesOpt.getOrElse(allAddresses)
}
