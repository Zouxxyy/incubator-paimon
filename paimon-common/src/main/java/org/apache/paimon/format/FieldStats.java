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

package org.apache.paimon.format;

import javax.annotation.Nullable;

import java.util.Objects;

/** Statistics for each field. */
public class FieldStats {

    @Nullable private Object minValue;
    @Nullable private Object maxValue;
    private Long nullCount;

    public FieldStats(
            @Nullable Object minValue, @Nullable Object maxValue, @Nullable Long nullCount) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.nullCount = nullCount;
    }

    @Nullable
    public Object minValue() {
        return minValue;
    }

    @Nullable
    public Object maxValue() {
        return maxValue;
    }

    @Nullable
    public Long nullCount() {
        return nullCount;
    }

    public static FieldStats empty() {
        return new FieldStats(null, null, null);
    }

    public void merge(FieldStats other) {
        if (other.minValue != null) {
            if (this.minValue == null) {
                this.minValue = other.minValue;
            } else {
                Comparable<Object> cl = (Comparable<Object>) this.minValue;
                Comparable<Object> cr = (Comparable<Object>) other.minValue;
                this.minValue = cl.compareTo(cr) < 0 ? cl : cr;
            }
        }

        if (other.maxValue != null) {
            if (this.maxValue == null) {
                this.maxValue = other.maxValue;
            } else {
                Comparable<Object> cl = (Comparable<Object>) this.maxValue;
                Comparable<Object> cr = (Comparable<Object>) other.maxValue;
                this.maxValue = cl.compareTo(cr) > 0 ? cl : cr;
            }
        }

        if (other.nullCount != null) {
            if (this.nullCount == null) {
                this.nullCount = other.nullCount;
            } else {
                this.nullCount += other.nullCount;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FieldStats)) {
            return false;
        }
        FieldStats that = (FieldStats) o;
        return Objects.equals(minValue, that.minValue)
                && Objects.equals(maxValue, that.maxValue)
                && Objects.equals(nullCount, that.nullCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minValue, maxValue, nullCount);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d}", minValue, maxValue, nullCount);
    }
}
