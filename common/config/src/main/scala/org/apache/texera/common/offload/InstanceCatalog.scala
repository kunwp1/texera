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

/**
  * A rentable instance type, e.g. one EC2 instance size.
  *
  * @param name         provider-specific identifier, e.g. "m5.large"
  * @param vcpu         virtual CPU count
  * @param memoryGiB    usable memory in GiB
  * @param pricePerHour on-demand price in USD per hour; 0.0 for a free local
  *                     stand-in used during development
  */
case class InstanceType(
    name: String,
    vcpu: Int,
    memoryGiB: Double,
    pricePerHour: Double
) {
  require(name != null && name.trim.nonEmpty, "instance name must be non-blank")
  require(vcpu > 0, s"$name: vcpu must be positive")
  require(memoryGiB > 0, s"$name: memoryGiB must be positive")
  require(pricePerHour >= 0, s"$name: pricePerHour must not be negative")

  def memoryBytes: Long = (memoryGiB * 1024 * 1024 * 1024).toLong
}

/**
  * The set of instance types an offloaded operator may be placed on.
  *
  * Selection minimises price subject to a hard memory floor. The asymmetry is
  * deliberate: an under-provisioned instance loses the whole run to an OOM kill,
  * whereas an over-provisioned one wastes only part of an instance-hour.
  */
case class InstanceCatalog(instances: Seq[InstanceType]) {
  require(instances.nonEmpty, "instance catalog must not be empty")
  require(
    instances.map(_.name).distinct.size == instances.size,
    "instance catalog must not contain duplicate names"
  )

  def byName(name: String): Option[InstanceType] = instances.find(_.name == name)

  /**
    * The cheapest instance whose memory covers `requiredBytes`.
    *
    * Returns None when nothing in the catalog is large enough, rather than
    * falling back to the largest instance: only the caller can decide whether to
    * abort or escalate, and a silent fallback would hide an OOM risk.
    */
  def cheapestFitting(requiredBytes: Long): Option[InstanceType] = {
    require(requiredBytes > 0, "requiredBytes must be positive")
    instances
      .filter(_.memoryBytes >= requiredBytes)
      // Tie-break on name so equal-priced candidates resolve deterministically,
      // keeping provisioning decisions reproducible across runs.
      .sortBy(i => (i.pricePerHour, i.name))
      .headOption
  }

  /**
    * `requiredBytes` inflated by `safetyFactor`, rounded up.
    *
    * The single definition of the headroom arithmetic. Callers pair it with
    * [[cheapestFitting]] rather than going through a combined helper, because the
    * one caller that matters also needs the inflated figure for its "nothing
    * fits" error -- so the number it reports is the number used to select.
    */
  def withHeadroom(requiredBytes: Double, safetyFactor: Double): Long = {
    require(requiredBytes > 0, "requiredBytes must be positive")
    require(safetyFactor >= 1.0, "safetyFactor must be at least 1.0")
    math.ceil(requiredBytes * safetyFactor).toLong
  }
}
