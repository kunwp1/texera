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

/** Raised when no catalog instance can hold an operator's estimated peak. */
class NoSuitableInstanceException(message: String) extends RuntimeException(message)

/**
  * The instance chosen for an offloaded operator, and why.
  *
  * The estimate fields are recorded so a provisioning decision can be audited
  * after the fact -- to compare what was predicted against what the operator
  * actually used.
  *
  * @param instanceType         the instance to rent
  * @param estimatedPeakBytes   the peak this decision was sized from, if any
  * @param safetyFactorApplied  headroom applied to that estimate, if any
  */
case class PlacementDecision(
    instanceType: InstanceType,
    estimatedPeakBytes: Option[Long] = None,
    safetyFactorApplied: Option[Double] = None
)

/**
  * Chooses which instance to rent for an offloaded operator.
  *
  * This runs before execution and before the user is billed, so every failure
  * here is cheap. The alternative -- discovering the instance was too small at
  * run time -- costs the whole run.
  */
class OffloadPlacementResolver(
    catalog: InstanceCatalog,
    defaultSafetyFactor: Double
) {

  /** Uses the instance type the user named. */
  def resolveManual(instanceTypeName: String): PlacementDecision = {
    require(
      instanceTypeName != null && instanceTypeName.trim.nonEmpty,
      "instance type must be non-blank"
    )
    val chosen = catalog
      .byName(instanceTypeName.trim)
      .getOrElse(
        throw new IllegalArgumentException(
          s"Unknown instance type '$instanceTypeName'. Available: " +
            catalog.instances.map(_.name).mkString(", ")
        )
      )
    PlacementDecision(chosen)
  }

  /**
    * Sizes the instance from an estimated peak memory requirement.
    *
    * @param estimatedPeakBytes the operator's estimated peak memory
    * @param safetyFactor       headroom to apply; defaults to the configured one
    */
  def resolveAdvised(
      estimatedPeakBytes: Long,
      safetyFactor: Option[Double]
  ): PlacementDecision = {
    val factor = safetyFactor.getOrElse(defaultSafetyFactor)
    // Catalog owns the headroom arithmetic and its guards; computing `required`
    // here (rather than re-deriving it) keeps the figure in the error message
    // identical to the one used for selection.
    val required = catalog.withHeadroom(estimatedPeakBytes.toDouble, factor)
    val chosen = catalog
      .cheapestFitting(required)
      .getOrElse(
        // Deliberately not the largest instance: presenting an almost-certain
        // OOM as a successful decision is worse than refusing to provision.
        throw new NoSuitableInstanceException(
          f"No instance can hold an estimated peak of ${bytesToGiB(estimatedPeakBytes)}%.2f GiB " +
            f"with a ${factor}%.2fx safety factor (${bytesToGiB(required)}%.2f GiB required). " +
            s"Largest available: ${catalog.instances.maxBy(_.memoryGiB).name}."
        )
      )
    PlacementDecision(chosen, Some(estimatedPeakBytes), Some(factor))
  }

  /**
    * Resolves a placement from a user's offload declaration.
    *
    * @param manualInstanceType the named type, required unless `advised`
    * @param advised            whether the platform sizes the instance
    * @param estimatedPeakBytes measured or predicted peak, required if `advised`
    * @param safetyFactor       per-operator headroom override
    */
  def resolve(
      manualInstanceType: Option[String],
      advised: Boolean,
      estimatedPeakBytes: Option[Long],
      safetyFactor: Option[Double]
  ): PlacementDecision = {
    if (advised) {
      val estimate = estimatedPeakBytes.getOrElse(
        throw new IllegalArgumentException(
          "Advised sizing requires an estimated peak memory; none was available."
        )
      )
      resolveAdvised(estimate, safetyFactor)
    } else {
      resolveManual(
        manualInstanceType.getOrElse(
          throw new IllegalArgumentException(
            "Manual sizing requires an instance type, but none was selected."
          )
        )
      )
    }
  }

  private def bytesToGiB(bytes: Long): Double = bytes.toDouble / (1024 * 1024 * 1024)
}
