# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Peak memory measurement for offloaded operators.

Used to measure what an operator actually consumed on subsampled inputs, so an
instance can be sized from measurement rather than from a guess.
"""

import resource
import sys


def _maxrss_bytes_multiplier(platform: str) -> int:
    """Bytes per unit of ``ru_maxrss`` on the given platform.

    getrusage reports ru_maxrss in different units per platform: kilobytes on
    Linux, bytes on macOS. Assuming one or the other yields a 1024x error in
    every memory estimate, which would pick an instance 1024x too large or --
    far worse -- too small.
    """
    if platform.startswith("darwin"):
        return 1
    # Linux and other Unix platforms report kilobytes.
    return 1024


def peak_memory_bytes() -> int:
    """Peak resident set size of this process, in bytes.

    This is a high-water mark for the whole process lifetime and never
    decreases, which is what instance sizing needs: the instance must hold the
    operator's largest moment, not its average.
    """
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return raw * _maxrss_bytes_multiplier(sys.platform)


def child_peak_memory_bytes() -> int:
    """Peak resident set size of terminated child processes, in bytes.

    A Python UDF may shell out to a subprocess; its peak is billed to the
    instance too, so sizing must account for it.
    """
    raw = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return raw * _maxrss_bytes_multiplier(sys.platform)
