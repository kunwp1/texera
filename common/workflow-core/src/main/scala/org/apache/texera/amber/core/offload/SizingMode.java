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

package org.apache.texera.amber.core.offload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;
import java.util.Arrays;

/**
 * How the instance for an offloaded operator is chosen.
 *
 * <p>A Java enum rather than a Scala sealed trait so the JSON schema generator
 * emits it as an "enum", which the property panel renders as a dropdown of the
 * valid modes instead of a free-text box.
 */
public enum SizingMode implements Serializable {

    /** The user names the instance type explicitly. */
    MANUAL("Manual"),

    /** The platform sizes the instance from an estimated peak memory requirement. */
    ADVISED("Advised");

    private final String name;

    SizingMode(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return this.name;
    }

    /**
     * Parses a mode from its string form, case-insensitively.
     *
     * <p>An unknown name is an error rather than a silent default: defaulting
     * could turn an intended Advised sizing into a Manual one with no instance
     * selected, which fails only later at provisioning time.
     */
    @JsonCreator
    public static SizingMode fromString(String value) {
        if (value != null) {
            String trimmed = value.trim();
            for (SizingMode mode : values()) {
                if (mode.name.equalsIgnoreCase(trimmed)) {
                    return mode;
                }
            }
        }
        throw new IllegalArgumentException(
                "Unknown sizing mode '"
                        + value
                        + "'. Valid values: "
                        + String.join(
                                ", ",
                                Arrays.stream(values()).map(SizingMode::getName).toArray(String[]::new)));
    }

    @Override
    public String toString() {
        return this.name;
    }
}
