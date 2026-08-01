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

package org.apache.texera.common.offload

import org.scalatest.flatspec.AnyFlatSpec

/**
  * Tests the rentable-instance catalog and the cheapest-safe-fit selection rule.
  *
  * Selection is a cost minimisation under a hard memory floor: among instances
  * whose memory covers the requirement, pick the cheapest. Under-provisioning
  * wastes an entire run to an OOM kill; over-provisioning wastes a fraction of
  * an instance-hour, so the rule must never trade safety for price.
  */
class InstanceCatalogSpec extends AnyFlatSpec {

  private val small = InstanceType("t3.small", vcpu = 2, memoryGiB = 2.0, pricePerHour = 0.0208)
  private val medium = InstanceType("t3.medium", vcpu = 2, memoryGiB = 4.0, pricePerHour = 0.0416)
  private val large = InstanceType("m5.large", vcpu = 2, memoryGiB = 8.0, pricePerHour = 0.096)
  // Deliberately cheaper per hour than m5.large but with the same memory, to
  // prove selection ranks on price and not on catalog order or family name.
  private val largeCheap =
    InstanceType("r6g.large", vcpu = 2, memoryGiB = 8.0, pricePerHour = 0.0806)

  private val catalog = InstanceCatalog(Seq(large, small, largeCheap, medium))

  // ---------------------------------------------------------------------------
  // InstanceType validation (negative)
  // ---------------------------------------------------------------------------

  "InstanceType" should "reject non-positive memory, vcpu, or price" in {
    assertThrows[IllegalArgumentException](InstanceType("bad", 2, 0.0, 0.1))
    assertThrows[IllegalArgumentException](InstanceType("bad", 2, -1.0, 0.1))
    assertThrows[IllegalArgumentException](InstanceType("bad", 0, 4.0, 0.1))
    assertThrows[IllegalArgumentException](InstanceType("bad", 2, 4.0, -0.1))
  }

  it should "reject a blank or null name" in {
    assertThrows[IllegalArgumentException](InstanceType("", 2, 4.0, 0.1))
    assertThrows[IllegalArgumentException](InstanceType("  ", 2, 4.0, 0.1))
    assertThrows[IllegalArgumentException](InstanceType(null, 2, 4.0, 0.1))
  }

  it should "allow a zero price, for a free local stand-in instance" in {
    val free = InstanceType("local-2g", 2, 2.0, 0.0)
    assert(free.pricePerHour == 0.0)
  }

  it should "expose memory in bytes for comparison against measured peaks" in {
    assert(medium.memoryBytes == 4L * 1024 * 1024 * 1024)
  }

  // ---------------------------------------------------------------------------
  // Catalog construction
  // ---------------------------------------------------------------------------

  "InstanceCatalog" should "reject an empty catalog" in {
    assertThrows[IllegalArgumentException](InstanceCatalog(Seq.empty))
  }

  it should "reject duplicate instance names" in {
    assertThrows[IllegalArgumentException](InstanceCatalog(Seq(medium, medium)))
  }

  it should "look up an instance by name" in {
    assert(catalog.byName("t3.medium").contains(medium))
    assert(catalog.byName("nonexistent").isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Cheapest safe fit (positive)
  // ---------------------------------------------------------------------------

  "cheapestFitting" should "pick the cheapest instance that covers the requirement" in {
    // 3 GiB needs more than t3.small; t3.medium is the cheapest that fits.
    assert(catalog.cheapestFitting(3L * 1024 * 1024 * 1024).contains(medium))
  }

  it should "rank on price, not on memory size or catalog order" in {
    // 5 GiB excludes t3.medium; both 8 GiB options fit, cheaper one must win.
    assert(catalog.cheapestFitting(5L * 1024 * 1024 * 1024).contains(largeCheap))
  }

  it should "accept an instance whose memory exactly equals the requirement" in {
    assert(catalog.cheapestFitting(medium.memoryBytes).contains(medium))
  }

  it should "pick the smallest instance for a tiny requirement" in {
    assert(catalog.cheapestFitting(1L).contains(small))
  }

  // ---------------------------------------------------------------------------
  // Cheapest safe fit (negative and boundary)
  // ---------------------------------------------------------------------------

  it should "return None when no instance is large enough" in {
    // Never silently return the largest instance: the caller must decide whether
    // to fail the run or escalate, rather than unknowingly accept an OOM risk.
    assert(catalog.cheapestFitting(64L * 1024 * 1024 * 1024).isEmpty)
  }

  it should "reject a non-positive requirement" in {
    assertThrows[IllegalArgumentException](catalog.cheapestFitting(0L))
    assertThrows[IllegalArgumentException](catalog.cheapestFitting(-1L))
  }

  it should "not be fooled by a requirement one byte above an instance's capacity" in {
    assert(catalog.cheapestFitting(medium.memoryBytes + 1).contains(largeCheap))
  }

  // ---------------------------------------------------------------------------
  // Safety factor -- the headroom applied before selection
  // ---------------------------------------------------------------------------

  "withHeadroom" should "inflate the requirement so a bigger instance is selected" in {
    // 3 GiB * 1.5 = 4.5 GiB, which no longer fits t3.medium.
    val required = catalog.withHeadroom(3.0d * 1024 * 1024 * 1024, safetyFactor = 1.5)
    assert(catalog.cheapestFitting(required).contains(largeCheap))
  }

  it should "leave the requirement unchanged when the factor is 1.0" in {
    val bytes = 3.0d * 1024 * 1024 * 1024
    assert(catalog.withHeadroom(bytes, 1.0) == bytes.toLong)
  }

  it should "round up, never down, so headroom is never silently lost" in {
    // Rounding down would hand back a requirement below the inflated peak.
    assert(catalog.withHeadroom(1000d, 1.0005) == 1001L)
  }

  it should "reject a safety factor below 1.0, which would shrink the requirement" in {
    assertThrows[IllegalArgumentException](catalog.withHeadroom(1024d, safetyFactor = 0.9))
  }

  it should "reject a non-positive requirement" in {
    assertThrows[IllegalArgumentException](catalog.withHeadroom(0d, safetyFactor = 1.25))
    assertThrows[IllegalArgumentException](catalog.withHeadroom(-1d, safetyFactor = 1.25))
  }
}
