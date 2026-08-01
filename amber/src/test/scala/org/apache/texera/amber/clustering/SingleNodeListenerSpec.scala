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

import org.apache.pekko.actor.{ActorSystem, Address, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

/**
  * Tests that the single-node listener answers every address query the deployment
  * path can send.
  *
  * Both queries are asked with a 5-second timeout. An unhandled message is not a
  * visible error -- the ask simply times out -- so a missing case would show up as
  * a stall on every deployment in single-node mode rather than as a failure.
  */
class SingleNodeListenerSpec
    extends TestKit(ActorSystem("SingleNodeListenerSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private val listener = system.actorOf(Props[SingleNodeListener]())

  "SingleNodeListener" should "answer GetAvailableNodeAddresses with its own address" in {
    listener ! ClusterListener.GetAvailableNodeAddresses()
    val addresses = expectMsgType[Array[Address]]
    assert(addresses.length == 1)
  }

  it should "answer GetGeneralPlacementAddresses too, not leave the ask to time out" in {
    // Single-node mode has no rented offload nodes, so the general pool is the
    // same single address.
    listener ! ClusterListener.GetGeneralPlacementAddresses()
    val addresses = expectMsgType[Array[Address]]
    assert(addresses.length == 1)
  }

  it should "report the same address for both queries" in {
    listener ! ClusterListener.GetAvailableNodeAddresses()
    val all = expectMsgType[Array[Address]]
    listener ! ClusterListener.GetGeneralPlacementAddresses()
    val general = expectMsgType[Array[Address]]
    assert(all.toSeq == general.toSeq)
  }
}
