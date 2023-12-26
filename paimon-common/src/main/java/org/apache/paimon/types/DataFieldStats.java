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
 *   <li>distinctRatio: distinct count / row count
 *   <li>avgLen: col average length
 *   <li>maxLen: col max length
 * </ul>
 */
public class DataFieldStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private final @Nullable Double distinctRatio;

    private final @Nullable Long avgLen;

    private final @Nullable Long maxLen;

    public DataFieldStats(
            @Nullable Double distinctRatio, @Nullable Long avgLen, @Nullable Long maxLen) {
        this.distinctRatio = distinctRatio;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
    }

    public @Nullable Double distinctRatio() {
        return distinctRatio;
    }

    public @Nullable Long avgLen() {
        return avgLen;
    }

    public @Nullable Long maxLen() {
        return maxLen;
    }

    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        if (distinctRatio != null) {
            generator.writeNumberField("distinctRatio", distinctRatio);
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
                jsonNode.get("distinctRatio") != null
                        ? jsonNode.get("distinctRatio").asDouble()
                        : null,
                jsonNode.get("avgLen") != null ? jsonNode.get("avgLen").asLong() : null,
                jsonNode.get("maxLen") != null ? jsonNode.get("maxLen").asLong() : null);
    }

    public DataFieldStats copy() {
        return new DataFieldStats(distinctRatio, avgLen, maxLen);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataFieldStats that = (DataFieldStats) o;
        return Objects.equals(distinctRatio, that.distinctRatio)
                && Objects.equals(avgLen, that.avgLen)
                && Objects.equals(maxLen, that.maxLen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(distinctRatio, avgLen, maxLen);
    }
}
