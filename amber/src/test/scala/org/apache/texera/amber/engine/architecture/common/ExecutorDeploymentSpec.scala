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

import org.apache.pekko.actor.Address
import org.apache.texera.amber.core.workflow.{
  PreferCoordinator,
  PreferPinnedAddress,
  RoundRobinPreference
}
import org.apache.texera.amber.engine.architecture.deploysemantics.AddressInfo
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Tests address resolution for worker placement, in particular the pinned
  * placement used by per-operator offloading.
  */
class ExecutorDeploymentSpec extends AnyFlatSpec {

  private val coordinator = Address("pekko", "Amber", "10.0.0.1", 2552)
  private val node1 = Address("pekko", "Amber", "10.0.1.1", 2552)
  private val node2 = Address("pekko", "Amber", "10.0.1.2", 2552)
  private val rented = Address("pekko", "Amber", "10.0.9.9", 2552)

  private def addressInfo(all: Address*): AddressInfo =
    AddressInfo(all.toArray, coordinator)

  // ---------------------------------------------------------------------------
  // Existing preferences must keep working unchanged
  // ---------------------------------------------------------------------------

  "resolveAddress" should "send PreferCoordinator to the coordinator address" in {
    val addr = ExecutorDeployment.resolveAddress(
      PreferCoordinator,
      addressInfo(node1, node2),
      workerIndex = 0
    )
    assert(addr == coordinator)
  }

  it should "spread RoundRobinPreference across nodes by worker index" in {
    val info = addressInfo(node1, node2)
    assert(ExecutorDeployment.resolveAddress(RoundRobinPreference, info, 0) == node1)
    assert(ExecutorDeployment.resolveAddress(RoundRobinPreference, info, 1) == node2)
    // wraps around
    assert(ExecutorDeployment.resolveAddress(RoundRobinPreference, info, 2) == node1)
  }

  // ---------------------------------------------------------------------------
  // A node rented for one offloaded operator must not receive general workers.
  // It is sized for exactly one operator, so a co-tenant's allocations would
  // invalidate that sizing and could get the offloaded operator OOM-killed --
  // the failure this feature exists to prevent.
  // ---------------------------------------------------------------------------

  it should "keep RoundRobinPreference off a dedicated offload node" in {
    val info = AddressInfo(
      allAddresses = Array(node1, rented, node2),
      coordinatorAddress = coordinator,
      generalPlacementAddressesOpt = Some(Array(node1, node2))
    )
    val placed =
      (0 until 6).map(i => ExecutorDeployment.resolveAddress(RoundRobinPreference, info, i))
    assert(!placed.contains(rented), s"rented node received general workers: $placed")
    assert(placed.toSet == Set(node1, node2))
  }

  it should "still pin to a dedicated offload node, which is not in the general pool" in {
    // Pinned placement resolves against the full membership on purpose; filtering
    // it would make the offloaded operator unplaceable on its own instance.
    val info = AddressInfo(
      allAddresses = Array(node1, rented, node2),
      coordinatorAddress = coordinator,
      generalPlacementAddressesOpt = Some(Array(node1, node2))
    )
    val addr = ExecutorDeployment.resolveAddress(PreferPinnedAddress(rented.toString), info, 0)
    assert(addr == rented)
  }

  it should "fail rather than place generally when every node is a dedicated offload node" in {
    // Silently falling back onto a dedicated node would defeat its sizing.
    val info = AddressInfo(
      allAddresses = Array(rented),
      coordinatorAddress = coordinator,
      generalPlacementAddressesOpt = Some(Array.empty)
    )
    assertThrows[IllegalStateException](
      ExecutorDeployment.resolveAddress(RoundRobinPreference, info, 0)
    )
  }

  it should "treat all nodes as generally placeable when no dedicated set is given" in {
    // Default for callers with no offload nodes; keeps existing behaviour.
    val info = AddressInfo(Array(node1, node2), coordinator)
    assert(info.generalPlacementAddresses.toSeq == Seq(node1, node2))
  }

  it should "reject RoundRobinPreference when the cluster has no nodes" in {
    val ex = intercept[IllegalStateException] {
      ExecutorDeployment.resolveAddress(RoundRobinPreference, addressInfo(), 0)
    }
    assert(ex.getMessage.toLowerCase.contains("no available computation nodes"))
  }

  // ---------------------------------------------------------------------------
  // Pinned placement (positive)
  // ---------------------------------------------------------------------------

  "resolveAddress with PreferPinnedAddress" should "return the matching cluster member" in {
    val addr = ExecutorDeployment.resolveAddress(
      PreferPinnedAddress(rented.toString),
      addressInfo(node1, rented, node2),
      workerIndex = 0
    )
    assert(addr == rented)
  }

  it should "return the same pinned node for every worker index" in {
    val info = addressInfo(node1, rented, node2)
    val pref = PreferPinnedAddress(rented.toString)
    val resolved = (0 until 5).map(i => ExecutorDeployment.resolveAddress(pref, info, i))
    assert(resolved.distinct == Seq(rented))
  }

  it should "match the coordinator when the coordinator itself is pinned" in {
    val addr = ExecutorDeployment.resolveAddress(
      PreferPinnedAddress(coordinator.toString),
      addressInfo(coordinator, node1),
      workerIndex = 0
    )
    assert(addr == coordinator)
  }

  // ---------------------------------------------------------------------------
  // Pinned placement (negative) -- must fail loudly, never silently relocate.
  // A silent fallback would run the operator on a node that was not sized for
  // it, which is the OOM this feature exists to prevent.
  // ---------------------------------------------------------------------------

  it should "fail when the pinned node is not a cluster member" in {
    val ex = intercept[IllegalStateException] {
      ExecutorDeployment.resolveAddress(
        PreferPinnedAddress(rented.toString),
        addressInfo(node1, node2),
        workerIndex = 0
      )
    }
    assert(ex.getMessage.contains(rented.toString))
  }

  it should "fail when the cluster is empty" in {
    assertThrows[IllegalStateException] {
      ExecutorDeployment.resolveAddress(
        PreferPinnedAddress(rented.toString),
        addressInfo(),
        workerIndex = 0
      )
    }
  }

  it should "not fall back to a round-robin node on a miss" in {
    val ex = intercept[IllegalStateException] {
      ExecutorDeployment.resolveAddress(
        PreferPinnedAddress(rented.toString),
        addressInfo(node1, node2),
        workerIndex = 0
      )
    }
    // The message must name the offload failure, not read like a generic
    // capacity error, so the cause is unambiguous in execution logs.
    assert(ex.getMessage.toLowerCase.contains("pinned"))
  }

  it should "not match a node that differs only in port" in {
    val other = Address("pekko", "Amber", "10.0.9.9", 9999)
    assertThrows[IllegalStateException] {
      ExecutorDeployment.resolveAddress(
        PreferPinnedAddress(rented.toString),
        addressInfo(other),
        workerIndex = 0
      )
    }
  }

  it should "not match a node that differs only in host" in {
    val other = Address("pekko", "Amber", "10.0.9.8", 2552)
    assertThrows[IllegalStateException] {
      ExecutorDeployment.resolveAddress(
        PreferPinnedAddress(rented.toString),
        addressInfo(other),
        workerIndex = 0
      )
    }
  }
}
