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

package org.apache.texera.common.compiler

import org.apache.texera.amber.core.offload.SizingMode
import org.apache.texera.amber.core.workflow.{PhysicalPlan, PreferPinnedAddress}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.common.config.OffloadConfigSettings
import org.apache.texera.common.offload.{InstanceCatalog, InstanceType, OffloadPlacementResolver}

/**
  * Connects a user's per-operator offload declaration to the engine's placement.
  *
  * The flow is:
  * {{{
  *   LogicalOp.offload -> validate -> rent instance -> address
  *                     -> PreferPinnedAddress on every PhysicalOp
  * }}}
  *
  * Renting happens outside this class; the planner only decides what to rent and
  * where the result must be pinned, which keeps the decision testable without
  * provisioning anything.
  */
class OffloadPlanner(
    catalog: InstanceCatalog,
    defaultSafetyFactor: Double,
    offloadEnabled: Boolean
) {

  private val resolver = new OffloadPlacementResolver(catalog, defaultSafetyFactor)

  /** The operators in `ops` that the user marked for offloading. */
  def offloadedOperators(ops: Seq[LogicalOp]): Seq[LogicalOp] = ops.filter(_.isOffloaded)

  /**
    * Checks an offloaded operator's declaration and returns what it needs.
    *
    * Runs at compile time so a bad declaration is reported before execution
    * starts and before anything billable is rented.
    *
    * @throws IllegalArgumentException if the declaration cannot be provisioned
    */
  def validate(op: LogicalOp): InstanceType = {
    require(op.isOffloaded, s"operator '${op.operatorIdentifier.id}' is not marked for offloading")
    op.offload.validationError.foreach(msg =>
      throw new IllegalArgumentException(s"operator '${op.operatorIdentifier.id}': $msg")
    )
    resolver
      .resolve(
        manualInstanceType = op.offload.instanceType,
        advised = op.offload.sizingMode == SizingMode.ADVISED,
        // Advised sizing needs a measured peak, which only the memory advisor
        // can supply; until then only Manual mode can be validated up front.
        estimatedPeakBytes = None,
        // None falls through to the configured default-safety-factor; passing the
        // operator's value unconditionally would make that knob unreachable.
        safetyFactor = op.offload.safetyFactor
      )
      .instanceType
  }

  /**
    * Validates `op` if, and only if, it is marked for offloading.
    *
    * Lets the compiler call this for every operator without first testing
    * whether offloading applies.
    *
    * Gated on the same master switch as the runtime path: with offloading
    * disabled nothing is ever rented, so a half-filled declaration must not be
    * the thing that blocks a run the platform would have executed normally.
    */
  def validateIfOffloaded(op: LogicalOp): Unit =
    if (offloadEnabled && op.isOffloaded) validate(op)

  /**
    * Rewrites a compiled physical plan so every physical operator belonging to
    * an offloaded logical operator is pinned to its rented instance's address.
    *
    * A logical operator can expand to several physical operators; all of them
    * must land on the one rented node, so each is pinned.
    *
    * @param plan            the compiled physical plan
    * @param rentedAddresses logical operator id -> address of its rented instance
    * @throws IllegalStateException if an address names a logical operator with no
    *                               physical operator in the plan, which means the
    *                               rental and the plan have diverged
    */
  def pinPlan(plan: PhysicalPlan, rentedAddresses: Map[String, String]): PhysicalPlan = {
    if (rentedAddresses.isEmpty) return plan

    // An address with no operator means the rental and the plan disagree about
    // what is being run. Pinning nothing would silently execute the operator on
    // the shared cluster, which was not sized for it.
    val logicalOpIds = plan.operators.map(_.id.logicalOpId.id)
    rentedAddresses.keys.filterNot(logicalOpIds.contains).foreach { missing =>
      throw new IllegalStateException(
        s"Rented an instance for operator '$missing', but the physical plan has no " +
          s"operator by that id. Rental and plan have diverged; refusing to run un-pinned."
      )
    }

    // One pass over the operators, one Set rebuild, rather than re-deriving the
    // plan once per pinned operator.
    plan.copy(operators = plan.operators.map { physicalOp =>
      rentedAddresses
        .get(physicalOp.id.logicalOpId.id)
        .fold(physicalOp)(address =>
          physicalOp.withLocationPreference(Some(PreferPinnedAddress(address)))
        )
    })
  }

}

object OffloadPlanner {

  /**
    * The planner built from the platform's offload settings.
    *
    * Single construction point on purpose. Compile-time validation and runtime
    * rental must agree on the catalog, the safety factor and -- critically -- the
    * enable gate; wiring those three settings at two call sites is what let the
    * gates diverge before. The planner is stateless, so one instance is shared.
    *
    * Lazy so reading offload.conf is deferred to first use rather than running
    * during class initialization.
    */
  lazy val fromConfig: OffloadPlanner = new OffloadPlanner(
    OffloadConfigSettings.catalog,
    OffloadConfigSettings.defaultSafetyFactor,
    OffloadConfigSettings.enabled
  )
}
