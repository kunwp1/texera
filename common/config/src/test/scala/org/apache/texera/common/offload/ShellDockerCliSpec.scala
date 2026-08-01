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

import scala.collection.mutable
import scala.sys.process.ProcessLogger

/**
  * Tests the shell-backed Docker CLI with a fake process runner, so no docker
  * binary is required.
  */
class ShellDockerCliSpec extends AnyFlatSpec {

  private def cliWith(
      exit: Int,
      stdout: String = "",
      stderr: String = ""
  ): (ShellDockerCli, mutable.ListBuffer[Seq[String]]) = {
    val calls = mutable.ListBuffer.empty[Seq[String]]
    val runner: ShellDockerCli.ProcessRunner = (command, logger: ProcessLogger) => {
      calls += command
      if (stdout.nonEmpty) logger.out(stdout)
      if (stderr.nonEmpty) logger.err(stderr)
      exit
    }
    (new ShellDockerCli("docker", () => Set.empty, runner), calls)
  }

  "run" should "prefix the command with the docker binary and return trimmed stdout" in {
    val (cli, calls) = cliWith(exit = 0, stdout = "container-xyz\n")
    val out = cli.run(Seq("run", "-d", "img"))
    assert(out == "container-xyz")
    assert(calls.head == Seq("docker", "run", "-d", "img"))
  }

  it should "raise with stderr context on a non-zero exit" in {
    val (cli, _) = cliWith(exit = 125, stderr = "Cannot connect to the Docker daemon")
    val ex = intercept[RuntimeException](cli.run(Seq("run", "img")))
    assert(ex.getMessage.contains("125"))
    assert(ex.getMessage.contains("Docker daemon"))
  }

  "remove" should "issue docker rm -f" in {
    val (cli, calls) = cliWith(exit = 0)
    cli.remove("container-xyz")
    assert(calls.head == Seq("docker", "rm", "-f", "container-xyz"))
  }

  it should "raise when rm fails, so the provider's safeRemove can swallow it" in {
    val (cli, _) = cliWith(exit = 1, stderr = "No such container")
    assertThrows[RuntimeException](cli.remove("gone"))
  }

  "clusterMemberAddresses" should "delegate to the injected membership function" in {
    val addrs = Set("pekko://Amber@10.0.0.1:2552")
    val cli = new ShellDockerCli("docker", () => addrs, (_, _) => 0)
    assert(cli.clusterMemberAddresses() == addrs)
  }
}
