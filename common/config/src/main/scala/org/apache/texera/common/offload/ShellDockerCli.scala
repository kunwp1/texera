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

import scala.sys.process.{Process, ProcessLogger}

/**
  * A [[DockerCli]] backed by the local `docker` binary.
  *
  * Cluster membership is not something Docker knows, so the caller injects a
  * function that reads it from the running Pekko cluster (the coordinator's
  * ClusterListener). This keeps common/config free of any dependency on amber,
  * where the actor system lives.
  *
  * @param dockerBinary       path or name of the docker executable
  * @param memberAddresses    reads current Pekko cluster member addresses
  * @param processRunner      seam over process execution, overridable in tests
  */
class ShellDockerCli(
    dockerBinary: String,
    memberAddresses: () => Set[String],
    processRunner: ShellDockerCli.ProcessRunner = ShellDockerCli.defaultRunner
) extends DockerCli {

  override def run(command: Seq[String]): String = {
    val output = new StringBuilder
    val errput = new StringBuilder
    val exit = processRunner(
      dockerBinary +: command,
      ProcessLogger(
        line => output.append(line).append('\n'),
        line => errput.append(line).append('\n')
      )
    )
    if (exit != 0) {
      throw new RuntimeException(
        s"`$dockerBinary ${command.mkString(" ")}` exited $exit: ${errput.toString.trim}"
      )
    }
    output.toString.trim
  }

  override def remove(containerId: String): Unit = {
    // -f so a still-running container is killed; ignore output, but let a
    // non-zero exit surface so the caller's safeRemove can swallow it.
    run(Seq("rm", "-f", containerId))
    ()
  }

  override def clusterMemberAddresses(): Set[String] = memberAddresses()
}

object ShellDockerCli {

  type ProcessRunner = (Seq[String], ProcessLogger) => Int

  private val defaultRunner: ProcessRunner =
    (command, logger) => Process(command).!(logger)
}
