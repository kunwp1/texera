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

import sys

import pytest

from pytexera.offload.peak_memory import (
    _maxrss_bytes_multiplier,
    child_peak_memory_bytes,
    peak_memory_bytes,
)


class TestMaxRssUnits:
    """ru_maxrss units differ per platform; the wrong unit is a 1024x error."""

    def test_macos_reports_bytes(self):
        assert _maxrss_bytes_multiplier("darwin") == 1
        assert _maxrss_bytes_multiplier("darwin23") == 1

    def test_linux_reports_kilobytes(self):
        assert _maxrss_bytes_multiplier("linux") == 1024

    def test_unknown_platform_defaults_to_kilobytes(self):
        # Most Unix platforms follow the Linux convention, so default there
        # rather than to the macOS special case.
        assert _maxrss_bytes_multiplier("freebsd14") == 1024


class TestPeakMemory:
    def test_returns_a_positive_byte_count(self):
        peak = peak_memory_bytes()
        assert peak > 0
        # A live interpreter is over 1 MiB and under 100 GiB. A unit error would
        # put the value far outside this range.
        assert 1024 * 1024 < peak < 100 * 1024 * 1024 * 1024

    def test_reflects_a_large_allocation(self):
        before = peak_memory_bytes()
        size = 200 * 1024 * 1024
        block = bytearray(size)
        # Touch each page so the pages are actually resident, not just reserved.
        for offset in range(0, size, 4096):
            block[offset] = 1
        after = peak_memory_bytes()

        assert after >= before
        # Allow slack for allocator overhead, but require most of the
        # allocation to show up -- proving the unit conversion is right.
        assert after - before > size * 0.7
        del block

    def test_never_decreases_within_a_process(self):
        first = peak_memory_bytes()
        block = bytearray(50 * 1024 * 1024)
        for offset in range(0, len(block), 4096):
            block[offset] = 1
        del block
        # It is a high-water mark: freeing memory must not lower it, which is
        # exactly the semantics instance sizing needs.
        assert peak_memory_bytes() >= first

    @pytest.mark.skipif(
        sys.platform == "win32", reason="RUSAGE_CHILDREN is not available on Windows"
    )
    def test_child_peak_is_non_negative(self):
        # Zero when no child has terminated yet, which is valid, not an error.
        assert child_peak_memory_bytes() >= 0
