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

package org.apache.texera.amber.core.offload

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

/**
  * Per-operator declaration that this operator runs on its own rented instance.
  *
  * Attached to a logical operator and carried in the workflow document, so it
  * must tolerate absent fields: workflows saved before this feature existed have
  * no offload block, and must keep running unchanged.
  *
  * @param enabled      whether to offload this operator to a rented instance
  * @param instanceType provider instance type; required in [[SizingMode.MANUAL]]
  * @param sizingMode   how the instance type is chosen
  * @param safetyFactor per-operator override for the headroom multiplier applied
  *                     to an estimated peak before selecting an instance; must be
  *                     >= 1.0. None means "use offload.default-safety-factor" --
  *                     a hardcoded default here would shadow that config knob and
  *                     make it dead.
  *
  *                     Not shown in the property panel. It only has an effect in
  *                     ADVISED sizing, which needs an estimated peak that nothing
  *                     produces yet; in MANUAL sizing the instance is named
  *                     outright, so the multiplier is inert. A control that
  *                     cannot change the outcome is worse than no control. The
  *                     field and its plumbing stay so the memory advisor can
  *                     expose it once the number means something.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class OffloadConfig(
    enabled: Boolean = false,
    instanceType: Option[String] = None,
    sizingMode: SizingMode = SizingMode.MANUAL,
    // Option[Double] erases to Option[Object], so without an explicit content type
    // the schema generator emits a $ref to the empty `Object` definition and the
    // property panel can never hold a number.
    @JsonDeserialize(contentAs = classOf[java.lang.Double])
    safetyFactor: Option[Double] = None
) {
  require(
    instanceType.forall(_.trim.nonEmpty),
    "instanceType must be non-blank when supplied"
  )
  require(safetyFactor.forall(_ >= 1.0), "safetyFactor must be at least 1.0 when supplied")

  @JsonIgnore
  def isOffloaded: Boolean = enabled

  /**
    * Why this configuration cannot be provisioned, if it cannot.
    *
    * Kept separate from the constructor `require`s so the frontend can surface a
    * message on a half-filled form instead of failing deserialization.
    */
  @JsonIgnore
  def validationError: Option[String] = {
    if (!enabled) None
    else if (sizingMode == SizingMode.MANUAL && instanceType.isEmpty)
      Some("An instance type must be selected when sizing mode is Manual.")
    else None
  }
}
