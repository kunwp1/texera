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

import org.apache.pekko.actor.Address
import org.apache.pekko.cluster.MemberStatus
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Which cluster members the coordinator may hand work to.
  *
  * ==Why only Up==
  *
  * A rented offload node was treated as ready the moment it appeared in
  * `cluster.state.members`, which includes `Joining`. With
  * `leader-actions-interval = 10s` that window is ~8s wide -- wide enough for the
  * coordinator to deploy the operator, run it to completion, and tear it down before
  * the leader ever promotes the node.
  *
  * That matters because remote death watch does not deliver `Terminated` for an actor
  * on a member below `Up` (characterised in `RemoteGracefulStopSpec`). So region
  * teardown's `gracefulStop` timed out on a worker that had in fact stopped cleanly --
  * executions 50, 54 and 55 all wedged this way, each holding a rented instance.
  *
  * Placement has the same requirement for the same reason, so the filter is applied to
  * every address the coordinator publishes rather than only to the offload readiness
  * check.
  */
class ClusterMembershipFilterSpec extends AnyFlatSpec with Matchers {

  private def addr(port: Int): Address = Address("pekko", "Amber", "host", port)

  private val coordinator = addr(2552)
  private val joiningWorker = addr(2560)
  private val upWorker = addr(2561)

  "placeableAddresses" should "exclude a member that has not yet reached Up" in {
    val members = Seq(
      coordinator -> MemberStatus.Up,
      joiningWorker -> MemberStatus.Joining
    )

    ClusterListener.placeableAddresses(members) should contain only coordinator
  }

  it should "include a member once it reaches Up" in {
    val members = Seq(
      coordinator -> MemberStatus.Up,
      upWorker -> MemberStatus.Up
    )

    ClusterListener.placeableAddresses(members) should contain theSameElementsAs Seq(
      coordinator,
      upWorker
    )
  }

  it should "exclude members that are on their way out" in {
    // Leaving/Exiting/Down members can still appear in `state.members`. Handing an
    // operator to one buys the same failure as Joining, from the other end.
    val members = Seq(
      coordinator -> MemberStatus.Up,
      addr(2570) -> MemberStatus.Leaving,
      addr(2571) -> MemberStatus.Exiting,
      addr(2572) -> MemberStatus.Down
    )

    ClusterListener.placeableAddresses(members) should contain only coordinator
  }

  it should "exclude WeaklyUp, which is a member the leader has not converged on" in {
    // WeaklyUp exists precisely because the leader could not act, so its death-watch
    // guarantees are no better than Joining's. Admitting it would reopen this bug in
    // exactly the situation -- an unreachable node somewhere -- where teardown matters.
    val members = Seq(
      coordinator -> MemberStatus.Up,
      addr(2573) -> MemberStatus.WeaklyUp
    )

    ClusterListener.placeableAddresses(members) should contain only coordinator
  }
}
