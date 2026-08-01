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

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

/**
  * Tests for [[PreferPinnedAddress]], the location preference that pins an
  * operator's workers to one specific cluster node -- the placement primitive
  * behind per-operator offloading to a rented instance.
  */
class PreferPinnedAddressSpec extends AnyFlatSpec {

  private def newPhysicalOp(id: String): PhysicalOp =
    PhysicalOp.oneToOnePhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(id), "main"),
      WorkflowIdentity(1L),
      ExecutionIdentity(1L),
      OpExecInitInfo.Empty
    )

  private def roundTripJava(pref: LocationPreference): LocationPreference = {
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    out.writeObject(pref)
    out.close()
    val in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    val result = in.readObject().asInstanceOf[LocationPreference]
    in.close()
    result
  }

  // ---------------------------------------------------------------------------
  // Construction and identity (positive)
  // ---------------------------------------------------------------------------

  "PreferPinnedAddress" should "be a LocationPreference carrying the requested node address" in {
    val pref = PreferPinnedAddress("pekko://Amber@10.0.1.5:2552")
    val asBase: LocationPreference = pref
    assert(asBase.isInstanceOf[PreferPinnedAddress])
    assert(pref.nodeAddress == "pekko://Amber@10.0.1.5:2552")
  }

  it should "compare by value so equal addresses are equal preferences" in {
    val a = PreferPinnedAddress("pekko://Amber@10.0.1.5:2552")
    val b = PreferPinnedAddress("pekko://Amber@10.0.1.5:2552")
    val c = PreferPinnedAddress("pekko://Amber@10.0.1.6:2552")
    assert(a == b)
    assert(a.hashCode == b.hashCode)
    assert(a != c)
  }

  it should "be distinct from the cluster-wide preferences" in {
    val pinned: LocationPreference = PreferPinnedAddress("pekko://Amber@10.0.1.5:2552")
    assert(pinned != PreferCoordinator)
    assert(pinned != RoundRobinPreference)
  }

  // ---------------------------------------------------------------------------
  // Serializability -- preferences travel with the PhysicalOp between actors
  // ---------------------------------------------------------------------------

  it should "be Serializable and survive a serialization round trip" in {
    val pref = PreferPinnedAddress("pekko://Amber@10.0.1.5:2552")
    assert(pref.isInstanceOf[Serializable])
    assert(roundTripJava(pref) == pref)
  }

  // ---------------------------------------------------------------------------
  // Rejecting malformed input (negative) -- an unusable address must fail at
  // construction, not silently at deployment time.
  // ---------------------------------------------------------------------------

  it should "reject an empty or blank node address" in {
    assertThrows[IllegalArgumentException](PreferPinnedAddress(""))
    assertThrows[IllegalArgumentException](PreferPinnedAddress("   "))
    assertThrows[IllegalArgumentException](PreferPinnedAddress("\t\n"))
  }

  it should "reject a null node address" in {
    assertThrows[IllegalArgumentException](PreferPinnedAddress(null))
  }

  it should "preserve unicode and unusual-but-nonblank addresses verbatim" in {
    // Validation is deliberately shallow: only blankness is rejected here, so
    // that address-format decisions stay with Pekko's own parser.
    val odd = PreferPinnedAddress("pekko://Ambér@hôte-1:2552")
    assert(odd.nodeAddress == "pekko://Ambér@hôte-1:2552")
  }

  // ---------------------------------------------------------------------------
  // Integration with PhysicalOp
  // ---------------------------------------------------------------------------

  "PhysicalOp.withLocationPreference" should "store a PreferPinnedAddress" in {
    val pref = PreferPinnedAddress("pekko://Amber@10.0.1.5:2552")
    val op = newPhysicalOp("offloaded").withLocationPreference(Some(pref))
    assert(op.locationPreference.contains(pref))
  }

  it should "leave a pinned operator's other properties untouched" in {
    val base = newPhysicalOp("offloaded")
    val pinned = base.withLocationPreference(
      Some(PreferPinnedAddress("pekko://Amber@10.0.1.5:2552"))
    )
    assert(pinned.id == base.id)
    assert(pinned.workflowId == base.workflowId)
    assert(pinned.parallelizable == base.parallelizable)
  }

  it should "be replaceable back to a cluster-wide preference" in {
    val op = newPhysicalOp("offloaded")
      .withLocationPreference(Some(PreferPinnedAddress("pekko://Amber@10.0.1.5:2552")))
      .withLocationPreference(Some(RoundRobinPreference))
    assert(op.locationPreference.contains(RoundRobinPreference))
  }
}
