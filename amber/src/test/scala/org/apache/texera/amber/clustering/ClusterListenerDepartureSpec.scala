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
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Tests the expected-departure registry that separates a released offload
  * instance from a crashed node.
  *
  * A `MemberRemoved` event is normally a node failure, and the listener responds
  * by force-stopping every non-completed execution. Per-operator offloading makes
  * a node leaving routine, so releasing one workflow's container must not be
  * mistaken for a crash -- that would force-fail unrelated running executions and
  * overwrite an already-KILLED one with a spurious FAILED.
  */
class ClusterListenerDepartureSpec extends AnyFlatSpec {

  private def addr(host: String): Address = Address("pekko", "Amber", host, 2552)

  "an announced departure" should "be recognised as expected" in {
    val a = addr("10.0.9.1")
    ClusterListener.expectDeparture(a.toString)
    assert(ClusterListener.consumeExpectedDeparture(a))
  }

  it should "be consumed, so a later genuine failure at the same address is a failure" in {
    // Without consuming, a node released once would be permanently exempt from
    // failure handling at that address.
    val a = addr("10.0.9.2")
    ClusterListener.expectDeparture(a.toString)
    assert(ClusterListener.consumeExpectedDeparture(a))
    assert(!ClusterListener.consumeExpectedDeparture(a))
  }

  "an unannounced departure" should "not be treated as expected" in {
    // This is the genuine node-crash path; it must still reach failure handling.
    assert(!ClusterListener.consumeExpectedDeparture(addr("10.0.9.3")))
  }

  it should "not be confused with a different announced address" in {
    ClusterListener.expectDeparture(addr("10.0.9.4").toString)
    assert(!ClusterListener.consumeExpectedDeparture(addr("10.0.9.5")))
    // The announced one is still pending, untouched by the miss.
    assert(ClusterListener.consumeExpectedDeparture(addr("10.0.9.4")))
  }

  it should "distinguish addresses that differ only by port" in {
    val released = Address("pekko", "Amber", "10.0.9.6", 2552)
    val other = Address("pekko", "Amber", "10.0.9.6", 9999)
    ClusterListener.expectDeparture(released.toString)
    assert(!ClusterListener.consumeExpectedDeparture(other))
    assert(ClusterListener.consumeExpectedDeparture(released))
  }

  "announcing the same address twice" should "still only be consumable once" in {
    val a = addr("10.0.9.7")
    ClusterListener.expectDeparture(a.toString)
    ClusterListener.expectDeparture(a.toString)
    assert(ClusterListener.consumeExpectedDeparture(a))
    assert(!ClusterListener.consumeExpectedDeparture(a))
  }

  "many announcements that never arrive" should "not accumulate without bound" in {
    // A container killed externally leaves an announcement no MemberRemoved ever
    // consumes; entries must not pile up for the coordinator's lifetime.
    val before = ClusterListener.pendingDepartureCount
    (0 until 50).foreach(i => ClusterListener.expectDeparture(addr(s"10.0.50.$i").toString))
    assert(ClusterListener.pendingDepartureCount >= before)
    // Consuming them all returns the registry to its prior size.
    (0 until 50).foreach(i => ClusterListener.consumeExpectedDeparture(addr(s"10.0.50.$i")))
    assert(ClusterListener.pendingDepartureCount == before)
  }
}
