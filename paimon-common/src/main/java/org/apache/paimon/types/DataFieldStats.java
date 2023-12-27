/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.types;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Table level col stats, supports the following stats.
 *
 * <ul>
 *   <li>distinctCount: distinct count
 *   <li>nullCount: null count
 *   <li>avgLen: average length
 *   <li>maxLen: max length
 * </ul>
 *
 * <p>Todo: Support min, max
 */
public class DataFieldStats implements Serializable {
    private static final long serialVersionUID = 1L;
    private final @Nullable Long distinctCount;
    private final @Nullable Long nullCount;
    private final @Nullable Long avgLen;
    private final @Nullable Long maxLen;

    public DataFieldStats(
            @Nullable Long distinctCount,
            @Nullable Long nullCount,
            @Nullable Long avgLen,
            @Nullable Long maxLen) {
        this.distinctCount = distinctCount;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
    }

    public @Nullable Long avgLen() {
        return avgLen;
    }

    public @Nullable Long distinctCount() {
        return distinctCount;
    }

    public @Nullable Long maxLen() {
        return maxLen;
    }

    public @Nullable Long nullCount() {
        return nullCount;
    }

    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        if (distinctCount != null) {
            generator.writeNumberField("distinctCount", distinctCount);
        }
        if (nullCount != null) {
            generator.writeNumberField("nullCount", nullCount);
        }
        if (avgLen != null) {
            generator.writeNumberField("avgLen", avgLen);
        }
        if (maxLen != null) {
            generator.writeNumberField("maxLen", maxLen);
        }
        generator.writeEndObject();
    }

    public static DataFieldStats deserializeJson(JsonNode jsonNode) {
        return new DataFieldStats(
                jsonNode.get("distinctCount") != null
                        ? jsonNode.get("distinctCount").asLong()
                        : null,
                jsonNode.get("nullCount") != null ? jsonNode.get("nullCount").asLong() : null,
                jsonNode.get("avgLen") != null ? jsonNode.get("avgLen").asLong() : null,
                jsonNode.get("maxLen") != null ? jsonNode.get("maxLen").asLong() : null);
    }

    public DataFieldStats copy() {
        return new DataFieldStats(distinctCount, nullCount, avgLen, maxLen);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        DataFieldStats that = (DataFieldStats) object;
        return Objects.equals(distinctCount, that.distinctCount)
                && Objects.equals(nullCount, that.nullCount)
                && Objects.equals(avgLen, that.avgLen)
                && Objects.equals(maxLen, that.maxLen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(distinctCount, nullCount, avgLen, maxLen);
    }

    @Override
    public String toString() {
        return "DataFieldStats{"
                + "distinctCount="
                + distinctCount
                + ", nullCount="
                + nullCount
                + ", avgLen="
                + avgLen
                + ", maxLen="
                + maxLen
                + '}';
    }
}
