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

package org.apache.texera.amber.operator.metadata

import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.amber.operator.LogicalOp
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class OperatorMetadataGeneratorSpec extends AnyFlatSpec with Matchers {

  /**
    * Every schema node carrying `nullable`, paired with its JSON pointer.
    *
    * The frontend compiles each operator schema with Ajv before it can place the
    * operator on the canvas (WorkflowUtilService.getNewOperatorPredicate). Ajv
    * rejects `nullable` on a node that has no `type` -- the shape produced when a
    * complex-typed property is emitted as a bare `$ref` -- and the throw aborts
    * the add for *every* operator, not just the one that introduced it.
    */
  private def nullableNodesWithoutType(schema: JsonNode): Seq[String] = {
    def walk(node: JsonNode, path: String): Seq[String] = {
      val here =
        if (node.isObject && node.has("nullable") && !node.has("type")) Seq(path) else Seq.empty
      val children =
        if (node.isObject)
          node.fields().asScala.toSeq.flatMap(e => walk(e.getValue, s"$path.${e.getKey}"))
        else if (node.isArray)
          node.elements().asScala.toSeq.zipWithIndex.flatMap {
            case (n, i) => walk(n, s"$path[$i]")
          }
        else Seq.empty
      here ++ children
    }
    walk(schema, "$")
  }

  "A generated operator schema" should "never carry `nullable` on a node without `type`" in {
    val offenders = OperatorMetadataGenerator.operatorTypeMap.keys.toSeq.flatMap { opClass =>
      nullableNodesWithoutType(OperatorMetadataGenerator.generateOperatorJsonSchema(opClass))
        .map(path => s"${opClass.getSimpleName}: $path")
    }

    withClue(
      s"Ajv rejects these, breaking operator creation in the UI:\n${offenders.take(10).mkString("\n")}\n"
    ) {
      offenders shouldBe empty
    }
  }

  it should "emit a numeric type for an Option[Double] property" in {
    // Option[Double] erases to Option[Object], so without an explicit content type
    // the generator emits `$ref: #/definitions/Object` and no number ever
    // validates.
    //
    // Asserted against the raw generator rather than generateOperatorJsonSchema:
    // OffloadConfig.safetyFactor is the only Option[Double] in the tree, and the
    // panel-shaping step removes it (it cannot affect the outcome until the memory
    // advisor lands). The erasure hazard is a generator property, so it is pinned
    // where the generator produces it -- and this keeps guarding the next
    // Option[Double] that appears, wherever it appears.
    val raw = OperatorMetadataGenerator.jsonSchemaGenerator
      .generateJsonSchema(OperatorMetadataGenerator.operatorTypeMap.keys.head)
    val safetyFactor = raw.at("/definitions/OffloadConfig/properties/safetyFactor")

    safetyFactor.isMissingNode shouldBe false
    safetyFactor.path("type").asText() shouldBe "number"
    safetyFactor.has("$ref") shouldBe false
  }

  "OperatorMetadataGenerator.generateOperatorMetadata" should
    "throw a RuntimeException for a class that is not a registered operator type" in {
    // the abstract base LogicalOp is not one of the concrete subtypes registered via the
    // @JsonSubTypes list on LogicalOp, so it is never collected into operatorTypeMap
    OperatorMetadataGenerator.operatorTypeMap.contains(classOf[LogicalOp]) shouldBe false

    val ex = intercept[RuntimeException] {
      OperatorMetadataGenerator.generateOperatorMetadata(classOf[LogicalOp])
    }
    ex.getMessage should include(classOf[LogicalOp].toString)
    ex.getMessage should include("is not registered")
  }
}
