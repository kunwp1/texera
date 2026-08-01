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

package org.apache.texera.amber.engine.common

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.SerializationExtension
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.workflow.{
  LocationPreference,
  PreferCoordinator,
  PreferPinnedAddress,
  RoundRobinPreference
}
import org.apache.texera.common.config.PekkoConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

/**
  * Verifies location preferences survive the cluster's configured serializer.
  *
  * PhysicalOp (which carries the preference) is sent between the coordinator and
  * worker nodes, and the cluster runs with allow-java-serialization = off, so
  * Kryo is the only path. PreferPinnedAddress is the first case class among the
  * preferences -- the singletons before it could not lose state, but this one
  * carries an address that must arrive intact.
  */
class PinnedAddressSerializationSpec
    extends TestKit(
      // Must use the real cluster config: it is what binds java.io.Serializable
      // to Kryo and disables Java serialization.
      ActorSystem("PinnedAddressSerializationSpec", PekkoConfig.pekkoConfig)
    )
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def roundTrip(pref: LocationPreference): LocationPreference = {
    val serialization = SerializationExtension(system)
    val serializer = serialization.findSerializerFor(pref)
    val bytes = serializer.toBinary(pref)
    serialization
      .deserialize(bytes, serializer.identifier, pref.getClass.getName)
      .get
      .asInstanceOf[LocationPreference]
  }

  "PreferPinnedAddress" should "survive a serialization round trip with its address intact" in {
    val pref = PreferPinnedAddress("pekko://Amber@10.0.9.9:2552")
    val result = roundTrip(pref)
    assert(result == pref)
    assert(result.asInstanceOf[PreferPinnedAddress].nodeAddress == "pekko://Amber@10.0.9.9:2552")
  }

  it should "survive a round trip for a unicode host" in {
    val pref = PreferPinnedAddress("pekko://Ambér@hôte-1:2552")
    assert(roundTrip(pref) == pref)
  }

  it should "not be serialized by Java serialization, which the cluster disables" in {
    val serializer = SerializationExtension(system)
      .findSerializerFor(PreferPinnedAddress("pekko://Amber@10.0.9.9:2552"))
    assert(!serializer.getClass.getName.toLowerCase.contains("javaserializer"))
  }

  "The pre-existing preferences" should "still round trip as the same singletons" in {
    assert(roundTrip(PreferCoordinator) eq PreferCoordinator)
    assert(roundTrip(RoundRobinPreference) eq RoundRobinPreference)
  }
}
