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
  * Tests the step that turns a user's offload request into a concrete instance
  * choice, before anything is rented.
  */
class OffloadPlacementResolverSpec extends AnyFlatSpec {

  private val catalog = InstanceCatalog(
    Seq(
      InstanceType("t3.small", 2, 2.0, 0.0208),
      InstanceType("t3.medium", 2, 4.0, 0.0416),
      InstanceType("m5.large", 2, 8.0, 0.096),
      InstanceType("r5.large", 2, 16.0, 0.126)
    )
  )

  private val resolver = new OffloadPlacementResolver(catalog, defaultSafetyFactor = 1.25)

  // ---------------------------------------------------------------------------
  // Manual sizing (positive)
  // ---------------------------------------------------------------------------

  "resolveManual" should "return the instance the user named" in {
    assert(resolver.resolveManual("m5.large").instanceType.name == "m5.large")
  }

  it should "reject an instance type that is not in the catalog" in {
    // Renting an unknown type would fail at the provider with a less clear
    // error, after the user has already been told the run started.
    val ex = intercept[IllegalArgumentException](resolver.resolveManual("m5.24xlarge"))
    assert(ex.getMessage.contains("m5.24xlarge"))
  }

  it should "reject a blank instance type" in {
    assertThrows[IllegalArgumentException](resolver.resolveManual(""))
    assertThrows[IllegalArgumentException](resolver.resolveManual("  "))
  }

  // ---------------------------------------------------------------------------
  // Advised sizing from an estimated peak (positive)
  // ---------------------------------------------------------------------------

  "resolveAdvised" should "pick the cheapest instance covering the estimate plus headroom" in {
    // 3 GiB * 1.25 = 3.75 GiB -> t3.medium (4 GiB) is the cheapest that fits.
    val decision = resolver.resolveAdvised(3L * 1024 * 1024 * 1024, None)
    assert(decision.instanceType.name == "t3.medium")
  }

  it should "honour a per-operator safety factor over the default" in {
    // 3 GiB * 2.0 = 6 GiB, which no longer fits t3.medium.
    val decision = resolver.resolveAdvised(3L * 1024 * 1024 * 1024, Some(2.0))
    assert(decision.instanceType.name == "m5.large")
  }

  it should "record the estimate and the factor it applied, for auditability" in {
    val estimate = 3L * 1024 * 1024 * 1024
    val decision = resolver.resolveAdvised(estimate, Some(1.5))
    assert(decision.estimatedPeakBytes.contains(estimate))
    assert(decision.safetyFactorApplied.contains(1.5))
  }

  it should "leave estimate fields empty for a manual choice" in {
    val decision = resolver.resolveManual("m5.large")
    assert(decision.estimatedPeakBytes.isEmpty)
    assert(decision.safetyFactorApplied.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Advised sizing (negative and boundary)
  // ---------------------------------------------------------------------------

  it should "fail when no instance can hold the estimate" in {
    // Must not silently downgrade to the largest available instance: that would
    // present an almost-certain OOM as a successful provisioning decision.
    val ex = intercept[NoSuitableInstanceException] {
      resolver.resolveAdvised(500L * 1024 * 1024 * 1024, None)
    }
    assert(ex.getMessage.contains("500"))
  }

  it should "reject a non-positive estimate" in {
    assertThrows[IllegalArgumentException](resolver.resolveAdvised(0L, None))
    assertThrows[IllegalArgumentException](resolver.resolveAdvised(-1L, None))
  }

  it should "reject a safety factor below 1.0" in {
    assertThrows[IllegalArgumentException](resolver.resolveAdvised(1024L, Some(0.5)))
  }

  it should "pick the smallest instance for a tiny estimate" in {
    assert(resolver.resolveAdvised(1024L, None).instanceType.name == "t3.small")
  }

  it should "be exact at the boundary where headroom just exceeds an instance" in {
    val fourGiB = 4L * 1024 * 1024 * 1024
    // Exactly 4 GiB with no headroom fits t3.medium ...
    assert(resolver.resolveAdvised(fourGiB, Some(1.0)).instanceType.name == "t3.medium")
    // ... but one byte more does not.
    assert(resolver.resolveAdvised(fourGiB + 1, Some(1.0)).instanceType.name == "m5.large")
  }

  // ---------------------------------------------------------------------------
  // Dispatch from an OffloadConfig-shaped request
  // ---------------------------------------------------------------------------

  "resolve" should "use the named type in Manual mode and ignore any estimate" in {
    val decision = resolver.resolve(
      manualInstanceType = Some("r5.large"),
      advised = false,
      estimatedPeakBytes = Some(1024L),
      safetyFactor = None
    )
    assert(decision.instanceType.name == "r5.large")
  }

  it should "fail in Manual mode when no instance type was named" in {
    assertThrows[IllegalArgumentException] {
      resolver.resolve(None, advised = false, None, None)
    }
  }

  it should "fail in Advised mode when no estimate is available" in {
    // Advised sizing without a measurement has nothing to size from; guessing
    // here is exactly the failure mode this design avoids.
    assertThrows[IllegalArgumentException] {
      resolver.resolve(None, advised = true, None, None)
    }
  }
}
