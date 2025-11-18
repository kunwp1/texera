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

package org.apache.amber.core.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.amber.core.executor.OperatorExecutor;
import org.apache.texera.service.util.BigObjectManager;
import org.apache.texera.service.util.BigObjectStream;

import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

/**
 * BigObject represents a reference to a large object stored in S3.
 * The reference is formatted as a URI: s3://bucket/path/to/object
 */
public class BigObject {
    
    private final String uri;
    
    /**
     * Creates a BigObject from an S3 URI (primarily for deserialization).
     * 
     * @param uri S3 URI in the format s3://bucket/path/to/object
     */
    @JsonCreator
    public BigObject(@JsonProperty("uri") String uri) {
        if (uri == null || !uri.startsWith("s3://")) {
            throw new IllegalArgumentException("BigObject URI must start with 's3://' but was: " + uri);
        }
        this.uri = uri;
    }
    
    /**
     * Creates a new BigObject by uploading the stream to S3.
     * 
     * @param stream The input stream containing the data to store
     * @param executor The operator executor that provides execution context
     */
    public BigObject(InputStream stream, OperatorExecutor executor) {
        this(BigObjectManager.create(stream, executor.executionId(), executor.operatorId()).getUri());
    }
    
    @JsonValue
    public String getUri() {
        return uri;
    }
    
    public String getBucketName() {
        return URI.create(uri).getHost();
    }
    
    public String getObjectKey() {
        String path = URI.create(uri).getPath();
        return path.startsWith("/") ? path.substring(1) : path;
    }
    
    /**
     * Opens this big object for reading. Caller must close the returned stream.
     */
    public BigObjectStream open() {
        return BigObjectManager.open(this);
    }
    
    @Override
    public String toString() {
        return uri;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof BigObject)) return false;
        BigObject that = (BigObject) obj;
        return Objects.equals(uri, that.uri);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(uri);
    }
}
