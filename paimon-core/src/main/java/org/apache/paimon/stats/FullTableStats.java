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

package org.apache.paimon.stats;

import org.apache.paimon.format.FieldStats;
import org.apache.paimon.types.DataFieldStats;

import javax.annotation.Nullable;

/** All table stats, including table level field stats and file level field stats. */
public class FullTableStats {
    private final @Nullable DataFieldStats tableLevelFieldStats;
    private final FieldStats fileLevelFieldStats;

    public FullTableStats(@Nullable DataFieldStats globalFieldStats, FieldStats fileFieldStats) {
        this.tableLevelFieldStats = globalFieldStats;
        this.fileLevelFieldStats = fileFieldStats;
    }

    public @Nullable DataFieldStats tableLevelFieldStats() {
        return tableLevelFieldStats;
    }

    public FieldStats fileLevelFieldStats() {
        return fileLevelFieldStats;
    }
}
